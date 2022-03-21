import argparse
from os import path
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, explode, lit

DEFAULT_TWEET_COLUMNS = ["author_id", 
                        "conversation_id", 
                        "created_at", 
                        "id", 
                        "in_reply_to_user_id", 
                        "public_metrics.like_count",
                        "public_metrics.quote_count",
                        "public_metrics.reply_count",
                        "text"
                        ]

DEFAULT_USER_COLUMNS = [
    "created_at",
    "id",
    "name",
    "username"
]


def export_csv(df: DataFrame, dest: str):
    return df.coalesce(1).write.option("header", True).mode("overwrite").csv(dest)

def export_parquet(df: DataFrame, dest: str):
    return df.coalesce(1).write.option("header", True).mode("overwrite").parquet(dest)

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
    src: str,
    dest: str,
    processed_at: str,
    postgres_db: str,
    postgres_user: str,
    postgres_pwd: str,
):
    df = spark.read.json(src)

    formatted_dest = path.join(
        dest, "{table_name}", f"exported_date={processed_at}"
    )

    tweet_df = get_first_level(
        df=df, first_level_col="data", column_list=DEFAULT_TWEET_COLUMNS
    ).withColumn("processed_at", lit(processed_at))
    
    export_csv(tweet_df, formatted_dest.format(table_name="tweet"))
    load_data_to_postgres(tweet_df, 'twitter_staging.tweet', postgres_db, postgres_user, postgres_pwd)

    user_df = get_first_level(
        df=df, first_level_col="includes.users", column_list=DEFAULT_USER_COLUMNS
    ).withColumn("processed_at", lit(processed_at))

    export_csv(user_df, formatted_dest.format(table_name="user"))
    load_data_to_postgres(user_df, 'twitter_staging.user', postgres_db, postgres_user, postgres_pwd)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Spark Twitter Search tranformation"
    )
    parser.add_argument("--src", help="Source folder", required=True)
    parser.add_argument("--dest", help="Destination folder", required=True)
    parser.add_argument(
        "--processed-at", help="Processed timestamp", default=""
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

    args = parser.parse_args()

    spark = SparkSession.builder.appName(
        name="twitter_search_transformation"
    ).getOrCreate()

    twitter_search_transform(
        spark=spark,
        src=args.src,
        dest=args.dest,
        processed_at=args.processed_at,
        postgres_db=args.postgres_db,
        postgres_user=args.postgres_user,
        postgres_pwd=args.postgres_pwd,
    )