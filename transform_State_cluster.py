import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn to perform on each element in the input PCollection.
class CollectStateFn(beam.DoFn):
    def process(self, element):
        record = element
        state = record.get('state')

        return [(state, 1)]    

# DoFn to perform on each element in the input PCollection.
class ExpandStateFn(beam.DoFn):
    def process(self, element):
        state_id, notneeded = element 
        
        if state_id == "AL":
            statename = "ALABAMA"
            state_fips = 1
        elif state_id == "AK":
            statename = "ALASKA"
            state_fips = 2
        elif state_id == "AZ":
            statename = "ARIZONA"
            state_fips = 4
        elif state_id == "AR":
            statename = "ARKANSAS"
            state_fips = 5
        elif state_id == "CA":
            statename = "CALIFORNIA"
            state_fips = 6
        elif state_id == "CO":
            statename = "COLORADO"
            state_fips = 8
        elif state_id == "CT":
            statename = "CONNECTICUT"
            state_fips = 9
        elif state_id == "DE":
            statename = "DELEWARE"
            state_fips = 10
        elif state_id == "DC":
            statename = "DISTRICT OF COLUMBIA"
            state_fips = 11
        elif state_id == "FL":
            statename = "FLORIDA"
            state_fips = 12
        elif state_id == "GA":
            statename = "GEORGIA"
            state_fips = 13
        elif state_id == "HI":
            statename = "HAWAII"
            state_fips = 15
        elif state_id == "ID":
            statename = "IDAHO"
            state_fips = 16
        elif state_id == "IL":
            statename = "ILLINOIS"
            state_fips = 17
        elif state_id == "IN":
            statename = "INDIANA"
            state_fips = 18
        elif state_id == "IA":
            statename = "IOWA"
            state_fips = 19
        elif state_id == "KS":
            statename = "KANSAS"
            state_fips = 20
        elif state_id == "KY":
            statename = "KENTUCKY"
            state_fips = 21
        elif state_id == "LA":
            statename = "LOUISIANA"
            state_fips = 22
        elif state_id == "ME":
            statename = "MAINE"
            state_fips = 23
        elif state_id == "MD":
            statename = "MARYLAND"
            state_fips = 24
        elif state_id == "MA":
            statename = "MASSACHUSETTS"
            state_fips = 25
        elif state_id == "MI":
            statename = "MICHIGAN"
            state_fips = 26
        elif state_id == "MN":
            statename = "MINNESOTA"
            state_fips = 27
        elif state_id == "MS":
            statename = "MISSISSIPPI"
            state_fips = 28
        elif state_id == "MO":
            statename = "MISSOURI"
            state_fips = 29
        elif state_id == "MT":
            statename = "MONTANA"
            state_fips = 30
        elif state_id == "NE":
            statename = "NEBRASKA"
            state_fips = 31
        elif state_id == "NV":
            statename = "NEVADA"
            state_fips = 32
        elif state_id == "NH":
            statename = "NEW HAMPSHIRE"
            state_fips = 33
        elif state_id == "NJ":
            statename = "NEW JERSEY"
            state_fips = 34
        elif state_id == "NM":
            statename = "NEW MEXICO"
            state_fips = 35
        elif state_id == "NY":
            statename = "NEW YORK"
            state_fips = 36
        elif state_id == "NC":
            statename = "NORTH CAROLINA"
            state_fips = 37
        elif state_id == "ND":
            statename = "NORTH DAKOTA"
            state_fips = 38
        elif state_id == "OH":
            statename = "OHIO"
            state_fips = 39
        elif state_id == "OK":
            statename = "OKLAHOMA"
            state_fips = 40
        elif state_id == "OR":
            statename = "OREGON"
            state_fips = 41
        elif state_id == "PA":
            statename = "PENNSYLVANIA"
            state_fips = 42
        elif state_id == "RI":
            statename = "RHODE ISLAND"
            state_fips = 44
        elif state_id == "SC":
            statename = "SOUTH CAROLINA"
            state_fips = 45
        elif state_id == "SD":
            statename = "SOUTH DAKOTA"
            state_fips = 46
        elif state_id == "TN":
            statename = "TENNESSEE"
            state_fips = 47
        elif state_id == "TX":
            statename = "TEXAS"
            state_fips = 48
        elif state_id == "UT":
            statename = "UTAH"
            state_fips = 49
        elif state_id == "VT":
            statename = "VERMONT"
            state_fips = 50
        elif state_id == "VA":
            statename = "VIRGINIA"
            state_fips = 51
        elif state_id == "WA":
            statename = "WASHINGTON"
            state_fips = 53
        elif state_id == "WV":
            statename = "WEST VIRGINIA"
            state_fips = 54
        elif state_id == "WI":
            statename = "WISCONSIN"
            state_fips = 55
        elif state_id == "WY":
            statename = "WYOMING"
            state_fips = 56
        else:
            statename = "OTHER"
            state_fips = 0

        return [(state_id, statename, state_fips)]  

# PTransform: format for BQ sink
class MakeRecordFn(beam.DoFn):
    def process(self, element):
        state_id, statename, state_fips = element
        record = {'state_id':state_id, 
               'statename':statename, 
               'state_fips':state_fips}
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
    
    query_string = 'SELECT * FROM WildLand_workflow_modeled.State'
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query=query_string))

    # write PCollection to log file
    query_results | 'Write to log 1' >> WriteToText('query_results.txt')

    # apply a ParDo to the PCollection 
    state_pcoll = query_results | 'Extract Date' >> beam.ParDo(CollectStateFn())

    # write PCollection to log file
    state_pcoll | 'Write to log 2' >> WriteToText('state_count.txt')

    # apply GroupByKey to the PCollection
    group_pcoll = state_pcoll | 'Group by State' >> beam.GroupByKey()

    # write PCollection to log file
    group_pcoll | 'Write to log 3' >> WriteToText('group_by_state.txt')
  
    # apply a ParDo to the PCollection
    out_pcoll = group_pcoll | 'Expands State' >> beam.ParDo(ExpandStateFn())

    # write PCollection to a file
    out_pcoll | 'Write File' >> WriteToText('state_output.txt')
    
    # make BQ records
    bq_pcoll = out_pcoll | 'Make BQ Record' >> beam.ParDo(MakeRecordFn())
    
    qualified_table_name = PROJECT_ID + ':WildLand_workflow_modeled.State_Beam_DF'
    table_schema = 'state_id:STRING,statename:STRING,state_fips:INTEGER'
    
    bq_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))