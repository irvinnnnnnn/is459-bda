#!/usr/bin/env python3
"""Package the code that Terraform uploads. Run this before every `terraform apply`:

    python main/scripts/build.py

It writes main/build/ (not committed):
  q1-flink.zip    the Managed Flink application: main.py, q1_scoring.py, q1_core.py, aml_clean.py,
                  requirements.txt and lib/<Kinesis connector jar> (downloaded once from Maven Central)
  train.tar.gz    the SageMaker training script and its requirements
  schema.json     the columns of curated/live/scored/, taken from the code, so the Glue table
                  Terraform creates always matches what Flink writes
"""
import hashlib
import io
import json
import os
import sys
import tarfile
import urllib.request
import zipfile

MAIN = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
BUILD = os.path.join(MAIN, "build")
# Flink 1.19 on Managed Flink. This jar carries the 'kinesis' table connector used in main.py.
JAR = "flink-sql-connector-kinesis-4.3.0-1.19.jar"
JAR_URL = f"https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kinesis/4.3.0-1.19/{JAR}"
FIXED = (2024, 1, 1, 0, 0, 0)            # fixed timestamps: the same sources always give the same archive


def read(*parts):
    with open(os.path.join(MAIN, *parts), "rb") as fh:
        return fh.read()


def main():
    os.makedirs(BUILD, exist_ok=True)
    jar_path = os.path.join(BUILD, JAR)
    if not os.path.exists(jar_path):
        print(f"downloading {JAR} ...")
        urllib.request.urlretrieve(JAR_URL, jar_path + ".part")
        os.replace(jar_path + ".part", jar_path)
        expected = urllib.request.urlopen(JAR_URL + ".sha1").read().decode().split()[0]
        with open(jar_path, "rb") as fh:
            if hashlib.sha1(fh.read()).hexdigest() != expected:
                os.remove(jar_path)
                sys.exit("The downloaded jar does not match its published checksum. Run build.py again.")

    files = {"main.py": read("Q1_files", "flink", "main.py"), "q1_scoring.py": read("Q1_files", "flink", "q1_scoring.py"),
             "q1_core.py": read("Q1_files", "q1_core.py"), "aml_clean.py": read("common", "aml_clean.py"),
             "requirements.txt": read("Q1_files", "flink", "requirements.txt"),
             f"lib/{JAR}": open(jar_path, "rb").read()}
    with zipfile.ZipFile(os.path.join(BUILD, "q1-flink.zip"), "w", zipfile.ZIP_DEFLATED) as z:
        for name in sorted(files):
            z.writestr(zipfile.ZipInfo(name, FIXED), files[name], zipfile.ZIP_DEFLATED)

    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        for name, data in (("train.py", read("Q1_files", "sagemaker", "train.py")),
                           ("requirements.txt", read("Q1_files", "sagemaker", "requirements.txt"))):
            info = tarfile.TarInfo(name)
            info.size, info.mtime = len(data), 1704067200
            tar.addfile(info, io.BytesIO(data))
    import gzip
    with open(os.path.join(BUILD, "train.tar.gz"), "wb") as fh:
        with gzip.GzipFile(fileobj=fh, mode="wb", mtime=0) as gz:
            gz.write(buf.getvalue())

    sys.path[:0] = [os.path.join(MAIN, "common"), os.path.join(MAIN, "Q1_files"), os.path.join(MAIN, "Q1_files", "flink")]
    import q1_core
    import q1_scoring
    with open(os.path.join(BUILD, "schema.json"), "w") as fh:
        json.dump({"flink_jar": JAR, "features": q1_core.FEATURES,
                   "scored_columns": [list(c) for c in q1_scoring.scored_columns()]}, fh, indent=1)
    for name in ("q1-flink.zip", "train.tar.gz", "schema.json"):
        print(f"built main/build/{name} ({os.path.getsize(os.path.join(BUILD, name)) / 1e6:.2f} MB)")


if __name__ == "__main__":
    main()
