import datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

# source beam_venv_dir/bin/activate

default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2019, 12, 05)
}

staging_dataset = 'WildLand_workflow_staging'
modeled_dataset = 'WildLand_workflow_modeled'

bq_query_start = 'bq query --use_legacy_sql=false '

staging_schema = '\state:STRING,fdid:STRING,inc_date:INTEGER,inc_no:STRING,exp_no:INTEGER,version:STRING,latitude:FLOAT,longitude:FLOAT,township:FLOAT,north_sou:STRING,range:INTEGER,east_west:STRING,section:INTEGER,subsection:STRING,meridian:STRING,area_type:STRING,fire_cause:STRING,hum_fact1:STRING,hum_fact2:STRING,hum_fact3:STRING,hum_fact4:STRING,hum_fact5:STRING,hum_fact6:STRING,hum_fact7:STRING,hum_fact8:STRING,fact_ign1:STRING,fact_ign2:STRING,supp_fact1:STRING,supp_fact2:STRING,supp_fact3:,heat_sourc:STRING,mob_prop:STRING,eq_inv_ign:STRING,nfdrs_id:STRING,weath_type:STRING,wind_dir:STRING,wind_speed:INTEGER,air_temp:INTEGER,rel_humid:INTEGER,fuel_moist:INTEGER,dangr_rate:STRING,bldg_inv:INTEGER,bldg_thr:INTEGER,acres_burn:FLOAT,crop_burn1:STRING,crop_burn2:STRING,crop_burn3:STRING,undet_burn:INTEGER,tax_burn:INTEGER,notax_burn:INTEGER,local_burn:INTEGER,couty_burn:INTEGER,st_burn:INTEGER,fed_burn:INTEGER,forei_burn:INTEGER,milit_burn:INTEGER,other_burn:INTEGER,prop_manag:STRING,fed_code:STRING,nfdrs_fm:STRING,person_fir:STRING,gender:STRING,age:FLOAT,activity_w:STRING,horiz_dis:INTEGER,type_row:STRING,elevation:STRING,pos_slope:STRING,aspect:STRING,flame_lgth:INTEGER,spread_rat:INTEGER,serialid:INTEGER,latitude_longitude_appended:STRING'

create_date_sql = 'CREATE OR REPLACE TABLE ' + modeled_dataset + '''.Date AS 
                    SELECT * FROM (SELECT DISTINCT inc_date AS date 
                    FROM ''' + staging_dataset +'''.WildLandIncidents_2010 
                    UNION DISTINCT 
                    SELECT DISTINCT inc_date AS date 
                    FROM ''' + staging_dataset +'''.WildLandIncidents_2011) 
                    ORDER BY date'''

create_state_sql = 'CREATE OR REPLACE TABLE ' + modeled_dataset + '''.State AS 
                    SELECT * FROM (SELECT DISTINCT state 
                    FROM ''' + staging_dataset +'''.WildLandIncidents_2010 
                    UNION DISTINCT 
                    SELECT DISTINCT state 
                    FROM ''' + staging_dataset +'''.WildLandIncidents_2011) 
                    ORDER BY state'''

create_incidents_sql = 'CREATE OR REPLACE TABLE ' + modeled_dataset + '''.Incidents AS 
                    SELECT * FROM (SELECT serialid, state, fdid, inc_date AS date, inc_no, exp_no AS exposure_no, weath_type, 
                    wind_dir, wind_speed, air_temp, rel_humid, acres_burn, crop_burn1, crop_burn2, crop_burn3 
                    FROM ''' + staging_dataset +'''.WildLandIncidents_2010 
                    UNION DISTINCT 
                    SELECT DISTINCT serialid, state, fdid, inc_date AS date, inc_no, exp_no AS exposure_no, weath_type, 
                    wind_dir, wind_speed, air_temp, rel_humid, acres_burn, crop_burn1, crop_burn2, crop_burn3 
                    FROM ''' + staging_dataset +'''.WildLandIncidents_2010) 
                    ORDER BY serialid'''
                    

with models.DAG(
        'wildland_workflow',
        schedule_interval=None,
        default_args=default_dag_args) as dag:
    
    split = DummyOperator(
            task_id='split',
            trigger_rule='all_done')

    create_staging_dataset = BashOperator(
            task_id='create_staging_dataset',
            bash_command='bq --location=US mk --dataset ' + staging_dataset)
    
    create_modeled_dataset = BashOperator(
            task_id='create_modeled_dataset',
            bash_command='bq --location=US mk --dataset ' + modeled_dataset)
    
    load_2010 = BashOperator(
            task_id='load_2010',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.WildLandIncidents_2010 \
                         "gs://hermosillobazan_datasets/WildLand_Incidents_2010-2013/WildLandIncidents2010.csv"\
                         '+ staging_schema,
            trigger_rule='one_success')
    
    load_2011 = BashOperator(
            task_id='load_2011',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.WildLandIncidents_2011 \
                         "gs://hermosillobazan_datasets/WildLand_Incidents_2010-2013/WildLandIncidents2011.csv"\
                         '+ staging_schema,
            trigger_rule='one_success')
    
    create_date = BashOperator(
            task_id='create_date',
            bash_command=bq_query_start + "'" + create_date_sql + "'", 
            trigger_rule='one_success')
    
    create_state = BashOperator(
            task_id='create_state',
            bash_command=bq_query_start + "'" + create_state_sql + "'", 
            trigger_rule='one_success')
    
    create_incidents = BashOperator(
            task_id='create_incidents',
            bash_command=bq_query_start + "'" + create_incidents_sql + "'", 
            trigger_rule='one_success')
    
    #split_incidents = DummyOperator(
    #        task_id='split_incidents',
    #        trigger_rule='all_done')
    
    date_beam = BashOperator(
            task_id='date_beam',
            bash_command='python /home/jupyter/airflow/dags/transform_Date_single.py')
    
    date_dataflow = BashOperator(
            task_id='date_dataflow',
            bash_command='python /home/jupyter/airflow/dags/transform_Date_cluster.py')
    
    state_beam = BashOperator(
            task_id='state_beam',
            bash_command='python /home/jupyter/airflow/dags/transform_State_single.py')
    
    state_dataflow = BashOperator(
            task_id='state_dataflow',
            bash_command='python /home/jupyter/airflow/dags/transform_State_cluster.py')
    
    incidents_beam = BashOperator(
            task_id='incidents_beam',
            bash_command='python /home/jupyter/airflow/dags/transform_Incidents_single.py')
    
    incidents_dataflow = BashOperator(
            task_id='incidents_dataflow',
            bash_command='python /home/jupyter/airflow/dags/transform_Incidents_cluster.py')
    
    #create_staging_dataset >> create_modeled_dataset >> load_2010 >> load_2011 >> create_date >> create_state >> create_incidents >> date_beam >> date_dataflow >> state_beam >> state_dataflow >> incidents_beam >> incidents_dataflow
    
    create_staging_dataset >> create_modeled_dataset >> load_2010 >> load_2011 >> split
    split >> create_date >> date_beam >> date_dataflow
    split >> create_state >> state_beam >> state_dataflow
    split >> create_incidents >> incidents_beam >> incidents_dataflow