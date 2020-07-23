import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn to perform on each element in the input PCollection.
class CreatePrimaryKey(beam.DoFn):

    def process(self, element):
        record = element
        incident_id = record.get('serialid')   
        state = record.get('state')
        fdid = record.get('fdid')
        date = record.get('date')
        inc_no = record.get('inc_no')
        exposure_no = record.get('exposure_no')
        weath_type = record.get('weath_type')
        wind_dir = record.get('wind_dir')
        wind_speed = record.get('wind_speed')
        air_temp = record.get('air_temp')
        rel_humid = record.get('rel_humid')
        acres_burn = record.get('acres_burn')
        crop_burn1 = record.get('crop_burn1')
        crop_burn2 = record.get('crop_burn2')
        crop_burn3 = record.get('crop_burn3')
        
        d = str(date)
        if len(d) == 8:
            date = str(d[4:]+"-"+d[0]+d[1]+"-"+d[2]+d[3])
            
        if len(d) == 7:
            date = str(d[3:]+"-"+"0"+d[0]+"-"+d[1]+d[2])  
        incident_id = str(incident_id)+"-"+date[:4]
    
        return [(incident_id,state,fdid,date,inc_no,exposure_no,weath_type,wind_dir,wind_speed,air_temp,rel_humid,acres_burn,crop_burn1,crop_burn2,crop_burn3,)]



# PTransform: format for BQ sink
class MakeRecordFn(beam.DoFn):
    def process(self, element):
        incident_id, state, fdid, date, inc_no, exposure_no, weath_type, wind_dir, wind_speed, air_temp, rel_humid, acres_burn, crop_burn1, crop_burn2, crop_burn3 = element
        record = {'incident_id':incident_id,
                    'state':state,
                    'fdid':fdid,
                    'date':date,
                    'inc_no':inc_no,
                    'exposure_no':exposure_no,
                    'weath_type':weath_type,
                    'wind_dir':wind_dir,
                    'wind_speed':wind_speed,
                    'air_temp':air_temp,
                    'rel_humid':rel_humid,
                    'acres_burn':acres_burn,
                    'crop_burn1':crop_burn1,
                    'crop_burn2':crop_burn2,
                    'crop_burn3':crop_burn3}
        return [record] 
    
    
##FIX TTHIS
PROJECT_ID = 'trusty-wavelet-252622'
BUCKET = 'gs://wildland_incidents'

# Project ID is needed for BigQuery data source, even for local execution.
options = {
    'runner': 'DataflowRunner',
    'job_name': 'transform-state',
    'project': PROJECT_ID,
    'temp_location': BUCKET + '/temp',
    'staging_location': BUCKET + '/staging',
    'machine_type': 'n1-standard-4', # machine types listed here: https://cloud.google.com/compute/docs/machine-types
    'num_workers': 1
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create a Pipeline using a local runner for execution.
with beam.Pipeline('DataflowRunner', options=opts) as p:
    
    query_string = 'SELECT * FROM WildLand_workflow_modeled.Incidents'
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query=query_string))

    # write PCollection to log file
    query_results | 'Write to log 1' >> WriteToText('query_results.txt')

    # apply a ParDo to the PCollection 
    pkey_pcoll = query_results | 'Create primary key' >> beam.ParDo(CreatePrimaryKey())

    # write PCollection to log file
    pkey_pcoll | 'Write File' >> WriteToText('incident_output.txt')
    
    # make BQ records
    bq_pcoll = pkey_pcoll | 'Make BQ Record' >> beam.ParDo(MakeRecordFn())
    
    qualified_table_name = PROJECT_ID + ':WildLand_workflow_modeled.Incidents_Beam_DF'
    table_schema = 'incident_id:STRING,state:STRING,fdid:STRING,date:DATE,inc_no:STRING,exposure_no:INTEGER,weath_type:STRING,wind_dir:STRING,wind_speed:INTEGER,air_temp:INTEGER,rel_humid:INTEGER,acres_burn:FLOAT,crop_burn1:STRING,crop_burn2:STRING,crop_burn3:STRING'
    
    bq_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))