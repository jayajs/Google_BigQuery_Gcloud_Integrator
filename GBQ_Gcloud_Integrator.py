'''
Created on Jul 27, 2016

@author: user
'''
'''
Created on Jul 12, 2016

@author: jaya.r@brillio.com

'''
"""
code may be used to get one file
to gcloud and to query tables and then
query something -> store the output to
another table ->output as csv
"""

all_files_for_table = []
from oauth2client.client import GoogleCredentials
from googleapiclient.discovery import build
from googleapiclient import discovery
from googleapiclient import http
import uuid
import time
import json
from os import listdir
from os.path import isfile, join

from bigquery import schema_from_record


def create_gstorage_service():
    '''
    Function to create the biquery authentication object
    :return: a bigquery service object
    '''
    credentials = GoogleCredentials.get_application_default()
    return discovery.build('storage', 'v1', credentials=credentials)


def create_bquery_service():

    credentials = GoogleCredentials.get_application_default()
    bigquery_service = build('bigquery', 'v2', credentials=credentials)
    return bigquery_service


def upload_object(bucket, filename, readers, owners):
    '''
    Helper function to upload files to Google Cloud
    :param bucket:bucket_name in the google cloud
    :param filename: file to be opushed  on to google cloud
    :param readers:
    :param owners:
    :return: job
    '''
    service = create_gstorage_service()
    body = {
        'name': filename.split('\\')[-1],
    }

    # If specified, create the access control objects and add them to the
    # request body
    if readers or owners:
        body['acl'] = []

    for r in readers:
        body['acl'].append({
            'entity': 'user-%s' % r,
            'role': 'READER',
            'email': r
        })
    for o in owners:
        body['acl'].append({
            'entity': 'user-%s' % o,
            'role': 'OWNER',
            'email': o
        })

    with open(filename, 'rb') as f:
        req = service.objects().insert(
            bucket=bucket, body=body,
            media_body=http.MediaIoBaseUpload(f, 'application/octet-stream'))
        resp = req.execute()

    return resp


def load_table(bigquery, project_id, dataset_id, table_name, source_schema,
               source_path, num_retries=5):
    '''
    Function to load an empty table with Data
    :param bigquery: a Bigquery object
    :param project_id:The project_id where the table resides
    :param dataset_id:The data set where the table resides
    :param table_name: The table name to push the data
    :param source_schema:The source schema , currently not used
    :param source_path: The path to the original data
    :param num_retries: The number of retries before failing hard
    :return:
    '''
    job_data = {
        'jobReference': {
            'projectId': project_id,
            'job_id': str(uuid.uuid4())
        },
        'configuration': {
            'load': {
                'sourceUris': [source_path],
                #'schema': {
                #'fields': source_schema
                #},
                'destinationTable': {
                    'projectId': project_id,
                    'datasetId': dataset_id,
                    'tableId': table_name
                },
                "skipLeadingRows": 1,
                "autodetect": True,
            }
        }
    }
    return bigquery.jobs().insert(
        projectId=project_id,
        body=job_data).execute(num_retries=num_retries)


def poll_job(bigquery, job):
    '''
    Function to check the status of a running job
    :param bigquery: a Big Query object
    :param job: a Big Query Job
    :return: NA
    '''
    """Waits for a job to complete."""
    print('Waiting for job to finish...')
    request = bigquery.jobs().get(
        projectId=job['jobReference']['projectId'],
        jobId=job['jobReference']['jobId'])

    while True:
        result = request.execute(num_retries=5)

        if result['status']['state'] == 'DONE':
            if 'errorResult' in result['status']:
                raise RuntimeError(result['status']['errorResult'])
            print('Job complete.')
            return
        time.sleep(1)


def create_table(service, table_id, project_id, dataset_id, schema):
    '''
    Function to create a table object in google bigquery object
    :param service: the big query service object
    :param table_id: the table_id of the project
    :param project_id: a dataset_id where the table will be created
    :param dataset_id: A dataset to create a table in
    :param schema: a schema object for the table
    :return:
    '''

    dataset_ref = {'datasetId': dataset_id,
                   'projectId': project_id}

    table_ref = {'tableId': table_id,
                 'datasetId': dataset_id,
                 'projectId': project_id}
    tables = service.tables()
    # Now onto tables...
    table = {'tableReference': table_ref}
    table = tables.insert(body=table, **dataset_ref).execute()

    # Update the table to add a schema:
    #table['schema'] = {'fields': [{'name': 'a', 'type': 'string'}]}
    print schema

    table['schema'] = {'fields': schema}
    table = tables.update(body=table, **table_ref).execute()
    return table


def async_query_to_table(
        bigquery,
        project_id,
        query,
        datasetID,
        tableID,
        batch=False,
        num_retries=5):
    """
    Function creates an async query with the following parameters and the resulting data is
    stored on to to a table in the google BigQuery Service .
    :param bigquery: A bigquery object
    :param project_id: The project_ID of the projedct where the dataset should reside
    :param query: a SQL like query
    :param datasetID:A dataset ID in the Project ID
    :param tableID: A table name to store the data originating fro the  query
    :param batch:is this a batch query boolean
    :param num_retries: Number of retries before failing hard
    :return: Returns a result object which can be further queried to determine the sucess of the query
    """

    # Generate a unique job ID so retries
    # don't accidentally duplicate query
    job_data = {
        'jobReference': {
            'projectId': project_id,
            'jobId': str(uuid.uuid4())
        },
        'configuration': {
            'query': {
                'query': query,
                'priority': 'BATCH' if batch else 'INTERACTIVE',
                "destinationTable": {
                    "projectId": "e-collector-137823",
                    "datasetId": datasetID,
                    "tableId": tableID
                },
                "createDisposition": "CREATE_IF_NEEDED",
                "writeDisposition": "WRITE_TRUNCATE",
            }
        }
    }
    return bigquery.jobs().insert(
        projectId=project_id,
        body=job_data).execute(num_retries=num_retries)


def upload_files(folder_path , bucket_name):
    '''
    This function upload a set of csv files from a folder location to the google cloud
    :param folder_path:The path pointing to the location of csv files
    :param bucket_name : The destination bucket name on the google cloud, The bucket
    should exist before the method is called
    :return:
    '''
    files = [
        folder_path +
        "\\" +
        f for f in listdir(folder_path) if isfile(
            join(
                folder_path,
                f))]
    global all_files_for_table
    all_files_for_table = [
        f for f in listdir(folder_path) if isfile(
            join(
                folder_path, f))]
    print files
    for this_file in files:
        re = upload_object(
            bucket= bucket_name,
            filename=this_file,
            readers='',
            owners='')
        print(json.dumps(re, indent=2))


def load_tables_from_cloud(bservice):
    '''
    This module loads a set of datasets from
    google cloud service to google BIGQUERY for
    further evaluation
    :param bservice:a bigquery service object
    :param bucket_name :The bucket name of google cloud to fetch the data from eg 'nnbucket'
    :return: NA
    '''
    bucket_name = 'pst_mh'
    for tab in all_files_for_table:
        filename = tab
        table = filename.split('.')[0]
        source_path = 'gs://{0}/{1}'.format(bucket_name, filename)
        file1 = open('schema.json', 'r')
        schemafile = json.load(file1)
        file1.close()
        job = load_table(
            bigquery=bservice,
            project_id="e-collector-137823",
            dataset_id='pgst',
            table_name=table,
            source_schema=schemafile,
            source_path=source_path,
            num_retries=5)
        print json.dumps(job, indent=2)
        poll_job(bservice, job)


def getQueriesFromFile(filename):
    '''
    Function to grab all SQL queries from a sql file
    :param filename: The filename containing SQL Queries to be run on Google BigQuery
    :return: returns a array of cleaned queries
    '''
    # Single buffer
    fd = open(filename, 'r')
    sqlFile = fd.read()
    fd.close()

    # all SQL commands (split on ';')
    sqlCommands = sqlFile.split(';')
    cleaned_sqlCommands = []
    for c in sqlCommands:
        c = c.replace('\n', ' ')
        #c = c.strip()
        if c.strip():
            cleaned_sqlCommands.append(c)
    return cleaned_sqlCommands


def run():
    '''
    upload_files(folder_path = "D:\\Projects\\mh\\pst\\4GBQ")
    #transfer files from cloud to tables
    bservice = create_bquery_service()
    job = load_tables_from_cloud(bservice = bservice)
    print json.dumps(job , indent=2)
    '''
    project_id = 'e-collector-137823'
    dataset_id = 'Python_POSTGST'
    bservice = create_bquery_service()
    commands = executeScriptsFromFile('1.sql')
    counter = 0
    for i in commands:
        print i
        counter += 1
        resultDataset = "temp1_{0}".format(counter)
        res = async_query_to_table(
            bservice,
            project_id,
            i,dataset_id,
            tableID = resultDataset)
        poll_job(bservice, res)

    ##########################################################################

    '''bucket_name = 'mh_storage'
    re = upload_object(bucket = 'mh_storage', filename = 't1.csv', readers='', owners='')
    print(json.dumps(re, indent=2))
    ##########################################################################
    upload_files(folder_path = "D:\\Projects\\mh\\4GBQ")
    bservice = create_bquery_service()
    file1 = open('schema.json', 'r')
    schemafile = json.load(file1)
    file1.close()
    re = create_table(service = bservice, table_id = "test456", project_id = "e-collector-137823", dataset_id = "test1" ,schema=schemafile)
    ###########################################################################
    bservice = create_bquery_service()
    bucket_name = 'mh_storage'
    filename = "t1.csv"
    source_path =   'gs://{}/bq_TOLL.csv'.format(bucket_name,filename)
    file1 = open('schema.json', 'r')
    schemafile = json.load(file1)
    file1.close()
    job = load_table(bigquery = bservice, project_id = "e-collector-137823", dataset_id = 'pytest', table_name='test1', source_schema = schemafile, source_path=source_path, num_retries=5)
    print json.dumps(job , indent=2)
    poll_job(bservice, job)
    #print json.dumps(re,indent=2)
    ###########################################################################

    job = load_table(bigquery = bservice, project_id = "e-collector-137823", dataset_id = 'pytest', table_name='test', source_schema = schemafile, source_path=source_path, num_retries=5)
    print json.dumps(job , indent=2)
    poll_job(bservice, job)
    print json.dumps(re,indent=2)
    ###########################################################################

    bservice = create_bquery_service()
    query = 'SELECT * FROM [test1.test456];'
    res = async_query_to_table(bservice,"regal-extension-110907",query)
    print json.dumps(res,indent=2)
    poll_job(bservice, res)
    '''


def main():

    run()
main()
