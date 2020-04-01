import json, os, copy
import boto3
import pandas as pd


def state_submit(event, context):
    if 'states' not in event or 'makes' not in event:
        return json.dumps({
            'message': 'No search parameters supplied'
        })
    else:
        STATES = event['states']
        MAKES = event['makes']
    if os.environ.get('ENV') == 'local':
        from dotenv import load_dotenv
        load_dotenv()
        # set variables
        KEY = os.environ.get('AWS_KEY')
        SECRET = os.environ.get('AWS_SECRET')
        # create aws batch client
        batch = boto3.client('batch', region_name='us-east-1', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
        s3 = boto3.client('s3', region_name='us-east-1', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
        # ensure script should run
        print(f'Are you sure you would like to run scraper? [y|n]:')
        response = input()
        if response != 'y':
            print('Goodbye!')
            exit()
        else:
            print(f'Running scraper on states: {", ".join(STATES)}...')
    else:
        batch = boto3.client('batch', region_name='us-east-1')
        s3 = boto3.client('s3', region_name='us-east-1')

    # create job definition
    job = {
        'jobName': 'car-scraper',
        'jobDefinition': 'car-scraper:8',
        'jobQueue': 'cl-scraper',
        'containerOverrides': {
            'environment': [
                {
                    'name': 'S3_BUCKET',
                    'value': 'all-types-cl-data'
                },
                {
                    'name': 'SEARCH_TYPE',
                    'value': 'cars trucks'
                },
                {
                    'name': 'SEARCH_DEPTH',
                    'value': '2'
                },
                {
                    'name': 'VENDOR_TYPE',
                    'value': 'owner'
                },
                {
                    'name': 'GET_BODY',
                    'value': '1'
                },
                {
                    'name': 'ENV',
                    'value': 'production'
                }
            ]
        }
    }
    # get list of cities
    bucketName = 'all-types-cl-data'
    cityCsvName = 'cl_db/craigslist_cities.csv'
    city_obj = s3.get_object(Bucket=bucketName, Key=cityCsvName)
    df = pd.read_csv(city_obj['Body'])
    searches = df[df['state'].isin(STATES)]

    # placeholder for jobs to be submitted
    job_queue = []
    # create jobs to be submitted
    for make in MAKES:
        for i, city in searches.iterrows():
            _job = copy.deepcopy(job)

            _job['containerOverrides']['environment'].append({
            'name': 'SEARCH_CITY_URL',
            'value': city['link']
            })
            _job['containerOverrides']['environment'].append({
                'name': 'MAKE_MODEL',
                'value': make
            })
            _city = ''.join(s for s in city['city_name'] if s.isalnum())
            _job['jobName'] = f'{make}-{_city}'
            job_queue.append(_job)

    for j in job_queue:
        res = batch.submit_job(**j)
        print('SUBMITTED:', res['jobName'], 'JOB_ID:', res['jobId'])
    return json.dumps({
        'message': f'submitted {len(job_queue)} jobs'
    })
