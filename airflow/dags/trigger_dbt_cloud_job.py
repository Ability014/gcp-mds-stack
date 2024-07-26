# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from datetime import datetime

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.dbt.cloud.operators.dbt import (
    DbtCloudRunJobOperator,
)
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor

AIRBYTE_CONNECTION_ID = '6392102c-9758-4e31-9e49-19f2d467f274'

with DAG(
    dag_id="airbyte_to_dbt_job",
    default_args={"dbt_cloud_conn_id": "dbt-cloud", "account_id": 32309},
    start_date=datetime(2024, 7, 24),
    schedule_interval=None,
    catchup=False,
) as dag:

    trigger_airbyte_sync = AirbyteTriggerSyncOperator(
       task_id='airbyte_trigger_sync',
       airbyte_conn_id='airbyte-default',
       connection_id=AIRBYTE_CONNECTION_ID,
       asynchronous=True
    )

    wait_for_sync_completion = AirbyteJobSensor(
       task_id='airbyte_check_sync',
       airbyte_conn_id='airbyte-default',
       airbyte_job_id=trigger_airbyte_sync.output
    )

    run_this_last = EmptyOperator(
       task_id="job_sync_completed",
    )

    trigger_dbt_cloud_job_run = DbtCloudRunJobOperator(
        task_id="trigger_dbt_cloud_job_run",
        job_id=682241,
        check_interval=10,
        timeout=300,
    )

    trigger_airbyte_sync >> wait_for_sync_completion >> run_this_last >> trigger_dbt_cloud_job_run

