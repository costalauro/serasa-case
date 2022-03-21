import argparse
from os import path
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, explode, lit

def load_dw_table(dt_table: str, postgres_db: str, postgres_user: str, postgres_pwd: str):
    return (
        spark.read
        .format("jdbc")
        .option("url", postgres_db)
        .option("dbtable", dt_table)
        .option("user", postgres_user)
        .option("password", postgres_pwd)
        .load()
    )

def load_data_to_postgres(df:DataFrame, dt_table: str, postgres_db: str, postgres_user: str, postgres_pwd: str):
    (
    df.write
    .format("jdbc")
    .option("url", postgres_db)
    .option("dbtable", dt_table)
    .option("user", postgres_user)
    .option("password", postgres_pwd)
    .mode("overwrite")
    .save()
    )

def get_first_level(
    df: DataFrame, first_level_col: str, column_list: List[str]
):
    return (
        df.select(explode(col(first_level_col)))
        .select("col.*")
        .select(*column_list)
    )


def twitter_search_transform(
    spark: SparkSession,
    updated_at: str,
    postgres_db: str,
    postgres_user: str,
    postgres_pwd: str,
    dt_table: str
):
    twitter_staging_tweet_df = load_dw_table(
        dt_table=dt_table, postgres_db=postgres_db, postgres_user=postgres_user, postgres_pwd=postgres_pwd
    )

    twitter_staging_tweet_df.show()

    twitter_staging_user_df = load_dw_table(
        dt_table=dt_table, postgres_db=postgres_db, postgres_user=postgres_user, postgres_pwd=postgres_pwd
    )
    
    twitter_staging_user_df.show()

    # load_data_to_postgres(tweet_df, 'twitter.tweet', postgres_db, postgres_user, postgres_pwd)
    # load_data_to_postgres(user_df, 'twitter.user', postgres_db, postgres_user, postgres_pwd)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Spark Twitter Search tranformation to DW"
    )
    parser.add_argument(
        "--updated_at", help="Updated timestamp", default=""
    )
    parser.add_argument(
        "--postgres_db", help="postgres_db", default=""
    )
    parser.add_argument(
        "--postgres_user", help="postgres_db", default=""
    )
    parser.add_argument(
        "--postgres_pwd", help="postgres_db", default=""
    )
    parser.add_argument(
        "--dt_table", help="dt_table", default=""
    )

    args = parser.parse_args()

    spark = SparkSession.builder.appName(
        name="twitter_search_transformation_dw"
    ).getOrCreate()

    twitter_search_transform(
        spark=spark,
        updated_at=args.updated_at,
        postgres_db=args.postgres_db,
        postgres_user=args.postgres_user,
        postgres_pwd=args.postgres_pwd,
        dt_table=args.dt_table
    )