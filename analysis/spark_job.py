"""Run unchanged with local spark-submit or Amazon EMR Serverless.

A line 'A B' means A follows B. Duplicates must not inflate graph degrees.
Output is partitioned CSV plus a one-row metadata JSON directory.
"""
import argparse
import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.storagelevel import StorageLevel


def clean_edges(raw):
    parsed = (raw.select(F.trim('value').alias('line'))
              .withColumn('valid', F.col('line').rlike(r'^[0-9]+\s+[0-9]+$'))
              .withColumn('src_raw', F.regexp_extract('line', r'^([0-9]+)\s+([0-9]+)$', 1))
              .withColumn('dst_raw', F.regexp_extract('line', r'^([0-9]+)\s+([0-9]+)$', 2)))
    # Preserve IDs as strings (including arbitrarily large IDs); canonicalise leading zeros.
    for out, value in [('src', 'src_raw'), ('dst', 'dst_raw')]:
        stripped = F.regexp_replace(F.col(value), r'^0+', '')
        parsed = parsed.withColumn(out, F.when(stripped == '', F.lit('0')).otherwise(stripped))
    edges = (parsed.filter(F.col('valid') & (F.col('src') != F.col('dst')))
             .select('src', 'dst').dropDuplicates(['src', 'dst']))
    return parsed, edges


def count_users(edges):
    # Both directions emitted before a parallel grouped reduction. The union keeps
    # source-only and destination-only users; their missing direction stays zero.
    outgoing = edges.select(F.col('src').alias('user_id'), F.lit(0).alias('followers'), F.lit(1).alias('followees'))
    incoming = edges.select(F.col('dst').alias('user_id'), F.lit(1).alias('followers'), F.lit(0).alias('followees'))
    return (outgoing.unionByName(incoming).groupBy('user_id')
            .agg(F.sum('followers').alias('followers'), F.sum('followees').alias('followees')))


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--input', required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--partitions', type=int, default=8)
    p.add_argument('--overwrite', action='store_true', help='Only use for a disposable local run.')
    p.add_argument('--JOB_NAME', help='Injected by the AWS Glue managed runtime.')
    # Glue injects additional service flags. Keep strict argument checking locally.
    args = p.parse_known_args()[0] if '--JOB_NAME' in sys.argv else p.parse_args()
    if args.partitions < 2:
        p.error('Use at least two partitions to demonstrate parallel processing.')
    spark = SparkSession.builder.appName('CS5224-Twitter-Degrees').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    spark.conf.set('spark.sql.shuffle.partitions', args.partitions)
    spark.conf.set('spark.sql.adaptive.enabled', 'false')
    raw = spark.read.text(args.input).repartition(args.partitions)
    parsed, edges = clean_edges(raw)
    parsed.persist(StorageLevel.MEMORY_AND_DISK)
    edges.persist(StorageLevel.MEMORY_AND_DISK)
    counts = count_users(edges).persist(StorageLevel.MEMORY_AND_DISK)
    metrics = parsed.agg(
        F.count('*').alias('raw_lines'),
        F.sum(F.when(F.col('line') == '', 1).otherwise(0)).alias('blank_lines'),
        F.sum(F.when((F.col('line') != '') & ~F.col('valid'), 1).otherwise(0)).alias('malformed_lines'),
        F.sum(F.when(F.col('valid') & (F.col('src') == F.col('dst')), 1).otherwise(0)).alias('self_loop_lines'),
        F.sum(F.when(F.col('valid') & (F.col('src') != F.col('dst')), 1).otherwise(0)).alias('valid_nonself_lines'),
    ).first().asDict()
    metrics = {k: int(v or 0) for k, v in metrics.items()}
    metrics['unique_edges'] = edges.count()
    metrics['duplicate_edges_removed'] = metrics['valid_nonself_lines'] - metrics['unique_edges']
    totals = counts.agg(F.count('*').alias('unique_users'), F.sum('followers').alias('total_followers'),
                        F.sum('followees').alias('total_followees')).first().asDict()
    metrics.update({k: int(v or 0) for k, v in totals.items()})
    assert metrics['total_followers'] == metrics['total_followees'] == metrics['unique_edges']
    metrics.update(engine='Apache Spark', spark_version=spark.version,
                   execution_mode='local' if spark.sparkContext.master.startswith('local') else 'cloud/distributed',
                   master=spark.sparkContext.master, partitions=args.partitions, input_uri=args.input,
                   cleaning_policy='Numeric directed edges; canonical IDs; drop blanks, malformed rows, self-links; deduplicate (source,target).')
    mode = 'overwrite' if args.overwrite else 'errorifexists'
    counts.repartition(args.partitions).write.mode(mode).option('header', True).csv(args.output.rstrip('/') + '/users')
    spark.createDataFrame([metrics]).coalesce(1).write.mode(mode).json(args.output.rstrip('/') + '/metadata')
    print('AUDIT:', metrics)
    print('REQUIRED USERS:')
    counts.filter(F.col('user_id').isin('214328887', '107830991')).orderBy('user_id').show(truncate=False)
    counts.unpersist(); edges.unpersist(); parsed.unpersist()
    spark.stop()


if __name__ == '__main__':
    main()
