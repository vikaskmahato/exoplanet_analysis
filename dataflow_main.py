import re
import apache_beam as beam
# from apache_beam.io.gcp import bigquery
from google.cloud import bigquery
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions

# Set your Google Cloud project and GCS bucket information
project = 'airy-parsec-400006'
bucket = 'dataflow-apache-quickstart_airy-parsec-400006'
dataset_id = 'exoplanet_dataset'
table_id = 'planetary_systems_records'

# Define your Dataflow pipeline options, including your Google Cloud project
# options = PipelineOptions()
# google_cloud_options = options.view_as(GoogleCloudOptions)
# google_cloud_options.project = project

# set pipeline arguments => runner, gcp project details and gcs location 
# pipeline_args=[
#       '--runner=DataflowRunner',
#       '--job_name=job-gcs-to-bq',
#       '--project=airy-parsec-400006',
#       '--service_account_email=svc-rc-bq-practice@my-rcs-project-833123.iam.gserviceaccount.com',
#       '--region=us-central1',
#       '--temp_location=gs://rc-beam-test/temp',
#       '--staging_location=gs://rc-beam-test/stage'
#     ]
class Dataingestion:
    def parse_method(self, string_input):
        values = re.split(",", re.sub('\r\n', '', re.sub('"', '', string_input)))
        row = dict(zip(('pl_name', 'hostname', 'pl_orbper', 'pl_radj', 'pl_masse'), values))  
        return row

data_ingestion = Dataingestion()    
def run():
    options = PipelineOptions(
               save_main_session = True,
               runner = "DataflowRunner",
               project = "airy-parsec-400006",              
               temp_location = "gs://dataflow-apache-quickstart_airy-parsec-400006/temp",
               region = "us-central1" 
               )
    with beam.Pipeline(options=options) as p:
        # Read the CSV file from the GCS bucket
        input_data = (
            p
            | 'Read CSV from GCS' >> beam.io.ReadFromText(f'gs://{bucket}/exoplanet_PS_table_test_limit.csv')
            | 'String to BigQueryRow' >> beam.Map(lambda s:data_ingestion.parse_method(s))
        )

        # Transform the CSV data as needed (optional)
        # For example, you can use the "beam.Map" transform to clean or manipulate data

        # Define the BigQuery table schema (modify as needed)
        schema = 'pl_name:STRING,hostname:STRING,pl_orbper:FLOAT,pl_radj:FLOAT,pl_masse:FLOAT'

        # Write the data to BigQuery
        bq_data = (input_data | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            project=f'{project}',
            dataset=f'{dataset_id}',
            table=f'{table_id}',
            schema= schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,  # Choose write disposition as needed
            custom_gcs_temp_location = "gs://dataflow-apache-quickstart_airy-parsec-400006/temp"
        ))

if __name__ == "__main__":
    run()


