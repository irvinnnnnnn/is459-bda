"""Q1 Managed Apache Flink job (PyFlink): clean -> features -> score -> explain (spec 3.2).

    Kinesis ──▶ Parse ──▶ key by account ──▶ AccountFeatures ──▶ key by txn_id ──▶ Score ─┬─▶ S3 curated/live/scored/
    seed (S3) ─┘   └─▶ dlq/                  (sender side and receiver side)               └─▶ DynamoDB alerts

Each payment is handled twice by AccountFeatures, once under the sender's account and once under
the receiver's, so every feature comes from that one account's keyed state. Score joins the two
halves by txn_id. Windows use the payment's own timestamp (event time), and state is cleaned up by
event-time timers driven by watermarks, so an accelerated replay behaves like the real feed.

Runtime properties (set by Terraform, group "app"): bucket, stream.name, aws.region, our.bank,
alerts.table, audit.rate, init.position, seed.path, flush.seconds, flush.rows, sink.buckets.
"""
import json
import os
import time
from datetime import datetime, timezone

from pyflink.common import Duration, Types, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import KeyedProcessFunction, ProcessFunction, StreamExecutionEnvironment
from pyflink.datastream.state import ListStateDescriptor, ValueStateDescriptor
from pyflink.table import StreamTableEnvironment

HERE = os.path.dirname(os.path.abspath(__file__))
PROPS_FILE = "/etc/flink/application_properties.json"
IS_LOCAL = not os.path.exists(PROPS_FILE)
# Rejected records travel in the main output, marked with this key, and are split off with a filter.
# (PyFlink side outputs fail on Managed Flink when the operator is chained after a Table source.)
DLQ = "__dlq__"
DAY_MS = 86400 * 1000


def load_props():
    path = PROPS_FILE if not IS_LOCAL else os.path.join(HERE, "application_properties.json")
    with open(path) as fh:
        groups = {g["PropertyGroupId"]: g["PropertyMap"] for g in json.load(fh)}
    return groups.get("app", {})


class Parse(ProcessFunction):
    """Clean one record and keep it if it involves our bank. A kept record goes out twice, keyed for
    each account. Bad records go to dlq/. Other banks' payments are dropped."""

    def __init__(self, our_bank, seed):
        self.our_bank, self.seed = our_bank, seed

    def process_element(self, value, ctx):
        import aml_clean
        import q1_core as core
        from q1_scoring import dlq_line
        raw, arrived = (value, None) if self.seed else (value[0], value[1])
        try:
            p = aml_clean.clean_json(raw)
        except aml_clean.Reject as exc:
            if not self.seed:
                yield (DLQ, None, dlq_line(raw, str(exc)))
            return
        if not aml_clean.involves_bank(p, self.our_bank):
            return
        core.add_keys(p)
        p["seed"] = self.seed
        p["arrived_ms"] = _to_ms(arrived)
        yield (p["src"], "s", p)
        yield (p["dst"], "r", p)


def _to_ms(arrived):
    """Kinesis arrival time, sent from SQL as text in UTC."""
    try:
        d = datetime.fromisoformat(str(arrived).strip().replace(" ", "T")[:23])
        return int(d.replace(tzinfo=timezone.utc).timestamp() * 1000)
    except (ValueError, TypeError):
        return int(time.time() * 1000)


class EventTime(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return value[2]["ts"] * 1000


class AccountFeatures(KeyedProcessFunction):
    """Keyed by account. Holds that account's last 7 days and answers one side of each payment."""

    def open(self, ctx):
        self.state = ctx.get_state(ValueStateDescriptor("account", Types.PICKLED_BYTE_ARRAY()))

    def process_element(self, value, ctx):
        import q1_core as core
        _, role, p = value
        st = self.state.value() or core.new_state()
        feats = core.side_features(st, role, p)
        if len(st["ev"]) > 64:
            core.prune(st)
        self.state.update(st)
        day = p["ts"] * 1000 // DAY_MS * DAY_MS
        ctx.timer_service().register_event_time_timer(day + 8 * DAY_MS)     # one clean-up per account per day
        if not p["seed"]:
            yield (p["txn_id"], role, p, feats)

    def on_timer(self, timestamp, ctx):
        import q1_core as core
        st = self.state.value()
        if st is None:
            return ()
        if core.prune(st, timestamp // 1000):
            self.state.clear()
            return ()
        self.state.update(st)
        if not st["ev"]:                                                   # only remembered pairs are left
            ctx.timer_service().register_event_time_timer(timestamp + core.PAIR_DAYS * DAY_MS)
        return ()


class Score(KeyedProcessFunction):
    """Keyed by txn_id. Joins the sender half and the receiver half, scores, explains, writes alerts."""

    def __init__(self, props):
        self.props = props

    def open(self, ctx):
        from q1_scoring import Scorer
        p = self.props
        self.half = ctx.get_state(ValueStateDescriptor("half", Types.PICKLED_BYTE_ARRAY()))
        self.scorer = Scorer(p["bucket"], p["our.bank"], p["alerts.table"], float(p.get("audit.rate", 0.003)),
                             p["aws.region"], int(p.get("model.poll.seconds", 30)))

    def process_element(self, value, ctx):
        import q1_core as core
        _, role, p, feats = value
        other = self.half.value()
        if other is None:
            self.half.update((role, feats))
            return
        self.half.clear()
        if other[0] == role:                 # the same half twice (a replayed record): wait for the other
            self.half.update((role, feats))
            return
        row, item = self.scorer.score(p, {**other[1], **feats, **core.txn_features(p)}, p["arrived_ms"])
        if item is not None:
            self.scorer.put_alert(item)
        yield (hash(p["txn_id"]) % int(self.props.get("sink.buckets", 4)), row)


class S3Batch(KeyedProcessFunction):
    """Buffers rows in checkpointed state and writes them to S3 as one object per flush.

    A flush happens every flush.seconds, or sooner once flush.rows have built up. Because the buffer
    is Flink state, a restart re-writes rows rather than losing them (at least once); readers keep
    one row per txn_id.
    """

    def __init__(self, props, kind):
        self.props, self.kind = props, kind

    def open(self, ctx):
        import boto3
        self.buffer = ctx.get_list_state(ListStateDescriptor("rows", Types.PICKLED_BYTE_ARRAY()))
        self.armed = ctx.get_state(ValueStateDescriptor("armed", Types.BOOLEAN()))
        self.s3 = boto3.client("s3", region_name=self.props["aws.region"])
        self.tag = str(ctx.get_index_of_this_subtask())
        self.counts = {}

    def process_element(self, value, ctx):
        key, row = value
        self.buffer.add(row)
        n = self.counts[key] = self.counts.get(key, 0) + 1
        if n >= int(self.props.get("flush.rows", 5000)):
            self._flush(key)
        elif not self.armed.value():
            self.armed.update(True)
            ts = ctx.timer_service().current_processing_time() + int(self.props.get("flush.seconds", 30)) * 1000
            ctx.timer_service().register_processing_time_timer(ts)
        return ()

    def on_timer(self, timestamp, ctx):
        self._flush(ctx.get_current_key())
        return ()

    def _flush(self, key):
        from q1_scoring import write_dlq, write_scored
        rows = list(self.buffer.get())
        if rows:
            (write_scored if self.kind == "scored" else write_dlq)(self.s3, self.props["bucket"], rows, self.tag)
        self.buffer.clear()
        self.armed.clear()
        self.counts[key] = 0


def main():
    props = load_props()
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)
    t_env.get_config().set("table.local-time-zone", "UTC")
    for f in ("aml_clean.py", "q1_core.py", "q1_scoring.py"):
        env.add_python_file("file:///" + os.path.join(HERE, f).replace("\\", "/"))
    env.set_python_requirements("file:///" + os.path.join(HERE, "requirements.txt").replace("\\", "/"))
    if IS_LOCAL:
        jars = [f for f in os.listdir(os.path.join(HERE, "lib")) if f.endswith(".jar")]
        env.add_jars(*["file:///" + os.path.join(HERE, "lib", j).replace("\\", "/") for j in jars])
        env.enable_checkpointing(60000)

    t_env.execute_sql(f"""
        CREATE TABLE transactions (
            data STRING,
            arrival_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
        ) WITH (
            'connector' = 'kinesis',
            'stream' = '{props["stream.name"]}',
            'aws.region' = '{props["aws.region"]}',
            'scan.stream.initpos' = '{props.get("init.position", "LATEST")}',
            'format' = 'raw'
        )""")
    live = t_env.to_data_stream(t_env.sql_query("SELECT data, CAST(arrival_time AS STRING) FROM transactions"))
    parsed = live.process(Parse(str(int(props["our.bank"])), False)).name("clean")
    halves = parsed.filter(lambda v: v[0] != DLQ).name("kept")
    seed_path = (props.get("seed.path") or "").strip()
    if seed_path:
        from pyflink.datastream.connectors.file_system import FileSource, StreamFormat
        source = FileSource.for_record_stream_format(StreamFormat.text_line_format(), seed_path).build()
        seed = env.from_source(source, WatermarkStrategy.no_watermarks(), "seed-last-7-days")
        halves = halves.union(seed.process(Parse(str(int(props["our.bank"])), True)).name("clean-seed"))

    watermarks = (WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(120))
                  .with_timestamp_assigner(EventTime()).with_idleness(Duration.of_seconds(60)))
    scored = (halves.assign_timestamps_and_watermarks(watermarks)
              .key_by(lambda v: v[0], key_type=Types.STRING())
              .process(AccountFeatures()).name("account-features").uid("account-features")
              .key_by(lambda v: v[0], key_type=Types.STRING())
              .process(Score(props)).name("score-and-explain").uid("score-and-explain"))
    (scored.key_by(lambda v: v[0], key_type=Types.INT())
     .process(S3Batch(props, "scored")).name("sink-curated-live-scored").uid("sink-scored"))
    (parsed.filter(lambda v: v[0] == DLQ).map(lambda v: (0, v[2])).name("rejected")
     .key_by(lambda v: v[0], key_type=Types.INT())
     .process(S3Batch(props, "dlq")).name("sink-dlq").uid("sink-dlq"))
    env.execute("q1-realtime-flagging")


if __name__ == "__main__":
    main()
