import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# PTransform: Parse county names and create a county id, return (New county id (state+county fips), county name, state fips, county fips)
class ParseCountyNames(beam.DoFn):
  def process(self, element):
    record = element
    location = record.get('location')
    county_name = record.get('cz_name')
    state_code = record.get('state_fips')
    county_code = record.get('cz_fips')
    if county_name[-1] == ' ':
      county_name = county_name[-1]
    county_name = county_name.upper()
    county_id = str(state_code)+"-"+str(county_code)
    return [(county_id, [county_name, state_code, county_code])]

# PTransform: sum up nominations for a given actor/actress
class DeleteDuplicates(beam.DoFn):
  def process(self, element):
     county_id, county_info = element # count_obj is an _UnwindowedValues type
     county_info = list(county_info) # must cast to a list in order to call len()
     name = county_info[0][0]
     state_fips = county_info[0][1]
     county_fips = county_info[0][2]

     return [(county_id, name, state_fips, county_fips)]  

# PTransform: format for BQ sink
class MakeRecordFn(beam.DoFn):
  def process(self, element):
     county_id, name, state_fips,county_fips = element
     record = {'county_id': county_id, 
               'name':name, 
               'state_fips':state_fips,
               'county_fips':county_fips}
     return [record] 

PROJECT_ID = os.environ['PROJECT_ID']
BUCKET = os.environ['BUCKET']

# run pipeline on Dataflow 
options = {
    'runner': 'DataflowRunner',
    'job_name': 'transform-locations',
    'project': PROJECT_ID,
    'temp_location': BUCKET + '/temp',
    'staging_location': BUCKET + '/staging',
    'machine_type': 'n1-standard-4', # machine types listed here: https://cloud.google.com/compute/docs/machine-types
    'num_workers': 1
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create a Pipeline using a local runner for execution.
with beam.Pipeline('DataflowRunner', options=opts) as p:

    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM storm_events_modeled.Locations'))

    # write PCollection to log file, initial records
    query_results | 'Write to log 1' >> WriteToText('input.txt')

    # apply ParDo to the PCollection
    locations_pcoll = query_results | 'Parse county names and create county id' >> beam.ParDo(ParseCountyNames())

    # write PCollection to log file
    locations_pcoll | 'Write to log 2' >> WriteToText('Locations_Beam_DF.txt')

    # apply GroupByKey to the PCollection
    group_pcoll = locations_pcoll  | 'Group by Location' >> beam.GroupByKey()

    # write PCollection to log file
    group_pcoll | 'Write to log 3' >> WriteToText('group_by_Location.txt')
  
    # apply ParDo to the PCollection
    del_pcoll = group_pcoll | 'Deleting duplicates' >> beam.ParDo(DeleteDuplicates())

    # write PCollection to a file
    del_pcoll | 'Write File' >> WriteToText('output.txt')

    # make BQ records
    out_pcoll = del_pcoll | 'Make BQ Record' >> beam.ParDo(MakeRecordFn())

    qualified_table_name = PROJECT_ID + ':storm_events_modeled.Locations_Beam_DF'
    table_schema = 'county_id:STRING,name:STRING,state_fips:INTEGER,county_fips:INTEGER'
    
    out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
