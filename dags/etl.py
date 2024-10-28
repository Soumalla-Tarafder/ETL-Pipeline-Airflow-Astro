from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hook.postgres import PostgresHook
import json
from airflow.utils.dates import days_ago



## Define the DAG

with DAG(
    das_id="nasa_apod_postgres",
    start_date = days_ago(1),
    schedule_interval = '@daily',
    catchup=False
)as dag:
    
    ## Define the tasks
    ## step 1 : Create the table if not exists
    
    @task
    def create_table():
        ## Initialize postgreshook
        postgres_hook = PostgresHook(postgres_conn_id = "my_postgres_connection")

        ## Query to create the table
        query = """
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """

        ## execute the table creation query
        postgres_hook.run(query)
        
    
    ## step 2 : Extract the data from nasa api(APOD DATA)[Extract Pipeline]
    # https://api.nasa.gov/planetary/apod?api_key=SUTfugfjAzSkMaL7ywswspqg29hYSQmzJ6XTvscT
    extract_apod = SimpleHttpOperator(
        task_id = 'extract_apod',
        http_conn_id='nasa_api', 
        endpoint='planetary/apod',
        method='GET',
        data={"api_key":"{{ conn.nasa_api.extra_dejson.api_key}} "},
        response_filter = lambda res:res.json()    
    )

    
    
    ## step 3 : Transform the data which we should save
    
    @task
    def tranform_data(response):
        apod_data = {
            'title' : response.get('title',''),
            'explanation' : response.get('explanation',''),
            'url' : response.get('url',''),
            'date' : response.get('date',''),
            'media_type' : response.get('media_type','')
                                   
        }
        
        return apod_data
    
    ## step 4 : Load the data to the postgres sql
    
    @task
    def load_apod_data(apod_data):
        ## Initialize the postgreshook
        
        postgres_hook = PostgresHook(postgres_conn_id = "my_postgres_connection")

        ## define sql insert query
        
        query = """
        INSERT INTO apod_data (title,explanation,url,date,media_type)
        values (%s, %s, %s, %s, %s)
        """

        postgres_hook.run(query,parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']
        ))
    ## step 5 : Verify the data DBViewer
    
    
    ## step 6 : Define the task dependencies
    create_table() >> extract_apod  ## Ensure the table is created before extraction
    response = extract_apod.output
    transform_data = tranform_data(response)  ## Ensure the data is transformed before loading
    load_apod_data(tranform_data)
    