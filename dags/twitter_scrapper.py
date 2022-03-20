from os import path
from pathlib import Path
from airflow import DAG
from airflow.utils.dates import days_ago

from operators.twitter_operator import TwitterOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.postgres_operator import PostgresOperator

TWEET_SEARCH_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
}

BASE_FOLDER = path.join(    
    "/usr/local/spark/resources/datalake/twitter_scrapper/{stage}/twitter_search/{partition}",
)
PARTITION_FOLDER = "exported_date={{ ts_nodash }}"
spark_master = "spark://spark:7077"
spark_app_name = "Spark Hello World"
file_path = "/usr/local/spark/resources/data/airflow.cfg"

with DAG(
    "twitter_scrapper",
    default_args=ARGS,
    schedule_interval="0 7 * * *",
    max_active_runs=1,
) as dag:

    twitter_search = TwitterOperator(
        task_id="get_twitter_covid",
        query="covid",
        file_path=path.join(
            BASE_FOLDER.format(stage="bronze", partition=PARTITION_FOLDER),
            "Twitter_{{ ds_nodash }}.json"
        ),
        start_time=(
            "{{"
            f" (execution_date - macros.timedelta(days=1) - macros.timedelta(hours=3)).strftime('{ TWEET_SEARCH_TIME_FORMAT }') "
            "}}"
        ),
        end_time=(
            "{{"
            f" (next_execution_date - macros.timedelta(hours=3)).strftime('{ TWEET_SEARCH_TIME_FORMAT }') "
            "}}"
        )
    )

    twitter_transform = SparkSubmitOperator(
        task_id="transform_twitter_covid",
        application="/usr/local/spark/app/twitter_search/transformation.py",
        name="twitter_search_transformation",
        conn_id="spark_default",
        application_args=[
            "--src",
            BASE_FOLDER.format(stage="bronze", partition=PARTITION_FOLDER),
            "--dest",
            BASE_FOLDER.format(stage="silver", partition=""),
            "--processed-at",
            "{{ ts_nodash }}",
        ],
        verbose=1,
        conf={"spark.master":spark_master}
    )

    # tweet_dwh_staging = PostgresBulkLoadOperator(
    #     task_id="tweet_dwh_staging",
    #     table_name="twitter_staging.tweet",
    #     fields=[
    #         "author_id",
    #         "conversation_id",
    #         "created_at",
    #         "id",
    #         "in_reply_to_user_id",
    #         "lang",
    #         "possibly_sensitive",
    #         "like_count",
    #         "quote_count",
    #         "reply_count",
    #         "source",
    #         "text",
    #         "processed_at",
    #     ],
    #     folder=BASE_FOLDER.format(
    #         stage="silver", partition=f"tweet/{PARTITION_FOLDER}/*.csv"
    #     ),
    # )

    # tweet_dwh_merge = PostgresOperator(
    #     task_id="tweet_dwh_merge",
    #     sql="queries/tweet_dwh_merge.sql",
    # )

    # user_dwh_staging = PostgresBulkLoadOperator(
    #     task_id="user_dwh_staging",
    #     table_name="twitter_staging.user",
    #     fields=[
    #         "created_at",
    #         "description",
    #         "id",
    #         "location",
    #         "name",
    #         "pinned_tweet_id",
    #         "profile_image_url",
    #         "protected",
    #         "followers_count",
    #         "following_count",
    #         "listed_count",
    #         "tweet_count",
    #         "url",
    #         "username",
    #         "verified",
    #         "processed_at",
    #     ],
    #     folder=BASE_FOLDER.format(
    #         stage="silver", partition=f"user/{PARTITION_FOLDER}/*.csv"
    #     ),
    # )

    # user_dwh_merge = PostgresOperator(
    #     task_id="user_dwh_merge",
    #     sql="queries/user_dwh_merge.sql",
    # )

    twitter_search >> twitter_transform
    # twitter_transform >> tweet_dwh_staging >> tweet_dwh_merge
    # twitter_transform >> user_dwh_staging >> user_dwh_merge