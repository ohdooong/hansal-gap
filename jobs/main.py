import argparse
from pyspark.sql import SparkSession

from base import read_input, init_df
from filter import DailyStatFilter, TopRepoFilter, TopUserFilter
from es import Es


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--target_date", default=None, help="optional:target date(yyyy-mm-dd)")
    args = parser.parse_args()

    spark = (SparkSession
        .builder
        .master("local")
        .appName("spark-sql")
        .getOrCreate())
    args.spark = spark
    args.input_path = f"/opt/bitnami/spark/data/{args.target_date}-*.json"

    df = read_input(args.spark, args.input_path)
    df = init_df(df)

    # daily stat filter
    stat_filter = DailyStatFilter(args)
    stat_df = stat_filter.filter(df)

    # top repo filter
    repo_filter = TopRepoFilter(args)
    repo_df = repo_filter.filter(df)

    # top user filter
    user_filter = TopUserFilter(args)
    user_df = user_filter.filter(df)

    # store data to ES
    es = Es("http://es:9200")
    es.write_df(stat_df, "daily-stats-2024/_doc")
    es.write_df(repo_df, "top-repo-2024/_doc")
    es.write_df(user_df, "top-user-2024/_doc")
