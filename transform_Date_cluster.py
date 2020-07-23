import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn to perform on each element in the input PCollection.
class CollectDateFn(beam.DoFn):
    def process(self, element):
        record = element
        date = record.get('date')

        return [(date, 1)]    

# DoFn to perform on each element in the input PCollection.
class SplitDateFn(beam.DoFn):
    def process(self, element):
        date_id, notneeded = element 
        date = date_id # must cast to a list in order to call len()
        year = date % 10000
        rest = date / 10000
        day = rest % 100
        month = rest / 100
        
        date_id = str(year)+"-"+str(month)+"-"+str(day)
        
        if month == 1:
            monthname = "January"
        elif month == 2:
            monthname = "Febuary"
        elif month == 3:
            monthname = "March"
        elif month == 4:
            monthname = "April"
        elif month == 5:
            monthname = "May"
        elif month == 6:
            monthname = "June"
        elif month == 7:
            monthname = "July"
        elif month == 8:
            monthname = "August"
        elif month == 9:
            monthname = "September"
        elif month == 10:
            monthname = "October"
        elif month == 11:
            monthname = "November"
        elif month == 12:
            monthname = "December"
        else:
            monthname = ""

        return [(date_id, monthname, month, day, year)]  

# PTransform: format for BQ sink
class MakeRecordFn(beam.DoFn):
    def process(self, element):
        date_id, monthname, month, day, year = element
        record = {'date':date_id, 
               'monthname':monthname, 
               'month':month,
               'day':day,
               'year':year}
        return [record] 

##FIX TTHIS
PROJECT_ID = 'trusty-wavelet-252622'
BUCKET = 'gs://wildland_incidents'

# Project ID is needed for BigQuery data source, even for local execution.
options = {
    'runner': 'DataflowRunner',
    'job_name': 'transform-date',
    'project': PROJECT_ID,
    'temp_location': BUCKET + '/temp',
    'staging_location': BUCKET + '/staging',
    'machine_type': 'n1-standard-4', # machine types listed here: https://cloud.google.com/compute/docs/machine-types
    'num_workers': 1
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create a Pipeline using a local runner for execution.
with beam.Pipeline('DataflowRunner', options=opts) as p:

    query_string = 'SELECT * FROM WildLand_workflow_modeled.Date'
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query=query_string))

    # write PCollection to log file
    query_results | 'Write to log 1' >> WriteToText('query_results.txt')

    # apply a ParDo to the PCollection 
    date_pcoll = query_results | 'Extract Date' >> beam.ParDo(CollectDateFn())

    # write PCollection to log file
    date_pcoll | 'Write to log 2' >> WriteToText('date_count.txt')

    # apply GroupByKey to the PCollection
    group_pcoll = date_pcoll | 'Group by Date' >> beam.GroupByKey()

    # write PCollection to log file
    group_pcoll | 'Write to log 3' >> WriteToText('group_by_date.txt')
  
    # apply a ParDo to the PCollection
    out_pcoll = group_pcoll | 'Splits Up Date' >> beam.ParDo(SplitDateFn())

    # write PCollection to a file
    out_pcoll | 'Write File' >> WriteToText('date_output.txt')
    
    # make BQ records
    bq_pcoll = out_pcoll | 'Make BQ Record' >> beam.ParDo(MakeRecordFn())
    
    qualified_table_name = PROJECT_ID + ':WildLand_workflow_modeled.Date_Beam_DF'
    table_schema = 'date:DATE,monthname:STRING,month:INTEGER,day:INTEGER,year:INTEGER'
    
    bq_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))