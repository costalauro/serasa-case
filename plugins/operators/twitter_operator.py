import json
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from os.path import join

from airflow.models import DAG, BaseOperator, TaskInstance
from airflow.utils.decorators import apply_defaults
from hooks.twitter_hook import TwitterHook


class TwitterOperator(BaseOperator):

    template_fields = [
        "query",
        "file_path",
        "start_time",
        "end_time"
    ]

    @apply_defaults
    def __init__(
        self,
        query,
        file_path,
        conn_id = None,
        start_time = None,
        end_time = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.query = query
        self.file_path = file_path
        self.conn_id = conn_id
        self.start_time = start_time
        self.end_time = end_time

    def create_parent_folder(self):
        Path(Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)

    def execute(self, context):
        hook = TwitterHook(
            query=self.query,
            conn_id=self.conn_id,
            start_time=self.start_time,
            end_time=self.end_time
        )        
        self.create_parent_folder()        
        with open(self.file_path, "w") as output_file:
            for pg in hook.run():
                json.dump(pg, output_file, ensure_ascii=False)
                output_file.write("\n")
