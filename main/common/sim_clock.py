"""The demo's shared clock: one simulated time that the producer, both Streamlit apps and the
nightly jobs all read, so they stay in the same "world" during an accelerated replay.

The producer owns the clock and stores four numbers in s3://<bucket>/state/sim_clock.json:

    sim_anchor    a simulated time (epoch seconds)
    wall_anchor   the real time at which the simulated time was sim_anchor
    speed         simulated seconds per real second
    running       false while the world is paused

Everyone else computes the same "now" from them:

    now = sim_anchor + (real time - wall_anchor) * speed        (sim_anchor while paused)

Nothing has to be sent between the programs while the clock runs, so they cannot drift apart.
With no clock file, now() is the real time, which is what a real deployment would use.
"""
import json
import time

CLOCK_KEY = "state/sim_clock.json"


def sim_now(state, wall=None):
    """The simulated time for a clock state (a dict as stored), or the real time if there is none."""
    wall = time.time() if wall is None else wall
    if not state:
        return wall
    if not state.get("running"):
        return float(state["sim_anchor"])
    return float(state["sim_anchor"]) + (wall - float(state["wall_anchor"])) * float(state["speed"])


class SimClock:
    def __init__(self, bucket, region=None, s3=None, cache_seconds=5):
        if s3 is None:
            import boto3
            s3 = boto3.client("s3", region_name=region)
        self.s3, self.bucket, self.cache_seconds = s3, bucket, cache_seconds
        self._state, self._read_at = None, 0.0

    # ---- readers
    def state(self, fresh=False):
        if fresh or time.time() - self._read_at > self.cache_seconds:
            try:
                body = self.s3.get_object(Bucket=self.bucket, Key=CLOCK_KEY)["Body"].read()
                self._state = json.loads(body)
            except self.s3.exceptions.NoSuchKey:
                self._state = None
            self._read_at = time.time()
        return self._state

    def now(self):
        return sim_now(self.state())

    def simulated(self):
        return self.state() is not None

    # ---- the producer's side
    def _write(self, state):
        state["updated_at"] = time.time()
        self.s3.put_object(Bucket=self.bucket, Key=CLOCK_KEY, Body=json.dumps(state).encode(),
                           ContentType="application/json")
        self._state, self._read_at = state, time.time()

    def start(self, sim_ts, speed, **extra):
        self._write({"sim_anchor": float(sim_ts), "wall_anchor": time.time(), "speed": float(speed),
                     "running": True, **extra})

    def pause(self, **extra):
        st = self.state(fresh=True)
        if st:
            self._write({**st, "sim_anchor": sim_now(st), "wall_anchor": time.time(), "running": False, **extra})

    def resume(self, speed=None):
        st = self.state(fresh=True)
        if st:
            self._write({**st, "wall_anchor": time.time(), "running": True,
                         "speed": float(speed or st["speed"])})

    def note(self, **extra):
        """Save progress markers (e.g. sent_through) without moving the clock."""
        st = self.state()
        if st:
            self._write({**st, **extra})

    def clear(self):
        self.s3.delete_object(Bucket=self.bucket, Key=CLOCK_KEY)
        self._state, self._read_at = None, time.time()
