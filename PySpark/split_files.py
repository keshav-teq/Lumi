from __future__ import annotations

import argparse
import math
import os
import sys

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
    _here = os.path.dirname(os.path.abspath(__file__))
    for _cand in (os.path.join(_here, "hadoop"),
                  os.path.join(os.path.dirname(_here), "hadoop"),
                  r"C:\hadoop"):
        if os.path.isfile(os.path.join(_cand, "bin", "winutils.exe")):
            os.environ["HADOOP_HOME"] = _cand
            break

from pyspark.sql import SparkSession


def main() -> None:
    parser = argparse.ArgumentParser(description="Split a text feed into row chunks")
    parser.add_argument("--input", required=True, help="absolute path of the source file")
    parser.add_argument("--output-dir", required=True, help="absolute path; must not exist")
    parser.add_argument("--chunk-lines", type=int, default=5000,
                        help="max lines per part file (default 5000)")
    args = parser.parse_args()

    if args.chunk_lines <= 0:
        raise SystemExit("--chunk-lines must be positive")

    spark = (SparkSession.builder
             .appName("lumi-split-files")
             .master("local[*]")
             .config("spark.pyspark.python", sys.executable)
             .config("spark.pyspark.driver.python", sys.executable)
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        rdd = spark.sparkContext.textFile(args.input)
        total = rdd.count()
        if total == 0:
            raise SystemExit(f"refusing to split empty file: {args.input}")
        num_chunks = max(1, math.ceil(total / args.chunk_lines))

        split = (rdd.zipWithIndex()
                 .map(lambda li: (li[1], li[0]))
                 .partitionBy(num_chunks, lambda idx: idx // args.chunk_lines)
                 .mapPartitions(lambda it: iter(sorted(it, key=lambda kv: kv[0])))
                 .map(lambda kv: kv[1]))

        if os.path.exists(args.output_dir):
            raise SystemExit(f"refusing to write into existing dir: {args.output_dir}")
        os.makedirs(args.output_dir)

        ext = os.path.splitext(args.input)[1]
        written = 0
        for idx, rows in enumerate(split.glom().toLocalIterator()):
            part = os.path.join(args.output_dir, f"part-{idx:05d}{ext}")
            with open(part, "w", encoding="utf-8", newline="\n") as fh:
                for line in rows:
                    fh.write(line + "\n")
            written += len(rows)
        with open(os.path.join(args.output_dir, "_SUCCESS"), "w", encoding="utf-8"):
            pass
        if written != total:
            raise SystemExit(f"internal error: wrote {written} of {total} lines")
        print(f"split {total} lines into {num_chunks} chunk(s) at {args.output_dir}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
