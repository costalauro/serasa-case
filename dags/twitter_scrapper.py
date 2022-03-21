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
postgres_driver_jar = "/usr/local/spark/resources/jars/postgresql-9.4.1207.jar"
postgres_db = "jdbc:postgresql://postgresdw/dwhdb"
postgres_user = "dwhdb"
postgres_pwd = "dwhdb"

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
            "--postgres_db",
            postgres_db,
            "--postgres_user",
            postgres_user,
            "--postgres_pwd",
            postgres_pwd
        ],
        verbose=1,
        conf={"spark.master":spark_master},
        jars=postgres_driver_jar,
        driver_class_path=postgres_driver_jar,
    )
    tweet_dwh_merge = SparkSubmitOperator(
        task_id="tweet_dwh_merge",
        application="/usr/local/spark/app/twitter_search/transformation_dw.py",
        name="twitter_search_transformation_dw",
        conn_id="spark_default",
        application_args=[
            "--updated_at",
            "{{ ts_nodash }}",
            "--postgres_db",
            postgres_db,
            "--postgres_user",
            postgres_user,
            "--postgres_pwd",
            postgres_pwd,
            "--dt_table",
            "twitter_staging.tweet"
        ],
        verbose=1,
        conf={"spark.master":spark_master},
        jars=postgres_driver_jar,
        driver_class_path=postgres_driver_jar,
    )

    user_dwh_merge = SparkSubmitOperator(
        task_id="user_dwh_merge",
        application="/usr/local/spark/app/twitter_search/transformation_dw.py",
        name="twitter_search_transformation_dw",
        conn_id="spark_default",
        application_args=[
            "--updated_at",
            "{{ ts_nodash }}",
            "--postgres_db",
            postgres_db,
            "--postgres_user",
            postgres_user,
            "--postgres_pwd",
            postgres_pwd,
            "--dt_table",
            "twitter_staging.user"
        ],
        verbose=1,
        conf={"spark.master":spark_master},
        jars=postgres_driver_jar,
        driver_class_path=postgres_driver_jar,
    )

    twitter_search >> twitter_transform
    twitter_transform >> tweet_dwh_merge
    twitter_transform >> user_dwh_merge