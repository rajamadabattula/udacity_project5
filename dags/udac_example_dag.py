from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,PostgresOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_table = PostgresOperator(
    task_id = "Creating Tables",
    dag = dag,
    sql = "create_talbles.sql",
    postgres_conn_id = "redshift"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "satging_events",
    aws_credentials_id = "aws_credentials",
    s3_bucket = "udacity-dend",
    s3_prefix = "log_data",
    region = "us-west-2",
    copy_json_option = "s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='staging_songs',
    dag=dag,
    table = "staging_songs",
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    s3_bucket = "udacity-dend",
    s3_prefix = "song_data",
    region = "us-west-2",
    copy_json_option = "auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table = "songplays",
    select_sql = SqlQueries.songplay_table_insert,
    redshift_conn_id = "redshift"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table = "users",
    select_sql = SqlQueries.user_table_insert,
    redshift_conn_id = "redshift"   
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id='redshift',
    table='songs',
    select_sql=SqlQueries.song_table_insert,
    dag=dag
)
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id='redshift',
    table='artists',
    select_sql=SqlQueries.artist_table_insert,
    append_insert=True,
    primary_key="artistid",
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id='redshift',
    table='time',
    select_sql=SqlQueries.time_table_insert,
    dag=dag
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    test_query='select count(*) from songs where songid is null;',
    expected_result=0,
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> create_table
create_table >> stage_events_to_redshift >> load_songplays_table
create_table >> stage_songs_to_redshift  >> load_songplays_table
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator