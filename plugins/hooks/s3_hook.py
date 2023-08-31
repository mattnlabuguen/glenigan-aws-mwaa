from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime
from airflow.utils.dates import days_ago
