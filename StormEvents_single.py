import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# PTransform: Merge state and czfips into a a single column in the main dataset
class MergeIntoCountyID(beam.DoFn):
  def process(self, element):
    record = element
    event_id = record.get('event_id')
    event_type = record.get('event_type')
    year = record.get('year')
    month_name = record.get('month_name')
    begin_day = record.get('begin_day')
    state_fips = record.get('state_fips')
    cz_fips = record.get('cz_fips')
    injuries_direct = record.get('injuries_direct')
    injuries_indirect = record.get('injuries_indirect')
    deaths_direct = record.get('deaths_direct')
    deaths_indirect = record.get('deaths_indirect')
    damage_crops = record.get('damage_crops')

    county_id = str(state_fips)+"-"+str(cz_fips)
    event_id, event_type, year, month_name, begin_day, county_id, injuries_direct, injuries_indirect, deaths_direct, deaths_indirect, damage_crops
    return [(event_id, event_type, year, month_name, begin_day, county_id, injuries_direct, injuries_indirect, deaths_direct, deaths_indirect, damage_crops)]


# PTransform: format for BQ sink
class MakeRecordFn(beam.DoFn):
  def process(self, element):
    event_id, event_type, year, month_name, begin_day, county_id, injuries_direct, injuries_indirect, deaths_direct, deaths_indirect, damage_crops = element
    record = {'event_id' : event_id, 
            'event_type' : event_type, 
            'year' : year, 
            'month_name' : month_name, 
            'begin_day' : begin_day, 
            'county_id' : county_id, 
            'injuries_direct' : injuries_direct, 
            'injuries_indirect' : injuries_indirect, 
            'deaths_direct' : deaths_direct, 
            'deaths_indirect' : deaths_indirect, 
            'damage_crops' : damage_crops}
    return [record] 

PROJECT_ID = os.environ['PROJECT_ID']

# Project ID is needed for BigQuery data source, even for local execution.
options = {
    'project': PROJECT_ID
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)


# Create a Pipeline using a local runner for execution.
with beam.Pipeline('DirectRunner', options=opts) as p:

    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM storm_events_modeled.StormEvents'))

    # write PCollection to log file, initial records
    query_results | 'Write to log 1' >> WriteToText('input.txt')

    # apply ParDo to the PCollection
    county_merge_pcoll = query_results | 'Merge state fips and county fips into a single form' >> beam.ParDo(MergeIntoCountyID())

    # write PCollection to log file
    county_merge_pcoll | 'Write to log 2' >> WriteToText('output.txt')

    # make BQ records
    out_pcoll = county_merge_pcoll | 'Make BQ Record' >> beam.ParDo(MakeRecordFn())

    qualified_table_name = PROJECT_ID + ':storm_events_modeled.Curated_StormEvents_Beam'
    table_schema = 'event_id:INTEGER,event_type:STRING,year:INTEGER,month_name:STRING,begin_day:INTEGER,county_id:STRING,injuries_direct:INTEGER,injuries_indirect:INTEGER,deaths_direct:INTEGER,deaths_indirect:INTEGER,damage_crops:INTEGER'
    
    out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
