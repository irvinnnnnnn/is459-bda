"""Helpers used by both Q2 jobs (the Glue ETL and the EMR scoring job), and by nothing in Q1."""


def s3_dates(s3, bucket, prefix, key="date"):
    """Values of <key>=... sub-folders under a prefix, e.g. the dates that exist in a dataset."""
    out, kw = [], {"Bucket": bucket, "Prefix": prefix, "Delimiter": "/"}
    while True:
        r = s3.list_objects_v2(**kw)
        out += [p["Prefix"][len(prefix):].rstrip("/").split("=", 1)[1] for p in r.get("CommonPrefixes", [])
                if p["Prefix"][len(prefix):].startswith(key + "=")]
        if not r.get("IsTruncated"):
            return sorted(out)
        kw["ContinuationToken"] = r["NextContinuationToken"]
