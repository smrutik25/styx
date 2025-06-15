analytical_queries = [
    """SELECT COUNT(value) AS total_rows FROM ycsb;""",
    """SELECT SUM(value) AS total_sum FROM ycsb;""",
    """SELECT (value / 1000) AS bucket, COUNT(*) AS count
        FROM ycsb
        GROUP BY bucket
        ORDER BY bucket;""",
    """SELECT
          PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) AS p50,
          PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY value) AS p90,
          PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY value) AS p99
        FROM ycsb;""",
    """SELECT STDDEV_POP(value) AS std_dev, VAR_POP(value) AS variance
        FROM ycsb;"""
]