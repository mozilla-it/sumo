import argparse
import datetime
import json

import googleapiclient.discovery

from google.cloud import bigquery

# Explicitly use service account credentials by specifying the private
# key file. All clients in google-cloud-python have this helper.
client = bigquery.Client.from_service_account_json(
    'path/to/service_account.json')
    
def main():
    """Create a daily transfer from Standard to Nearline Storage class."""
    storagetransfer = googleapiclient.discovery.build('storagetransfer', 'v1')

    # Edit this template with desired parameters.
    transfer_job = {
        'description': 'SUMO playstore transfer',
        'status': 'ENABLED',
        'projectId': 'imposing-union-227917',
        'transferSpec': {
            'gcsDataSource': {
                'bucketName': 'pubsite_prod_rev_04753778179066947806'
            },
            'gcsDataSink': {
                'bucketName': sink_bucket
            },
            'objectConditions': {
                'minTimeElapsedSinceLastModification': '2592000s'  # 30 days
            },
            'transferOptions': {
                'deleteObjectsFromSourceAfterTransfer': 'true'
            }
        }
    }

    result = storagetransfer.transferJobs().create(body=transfer_job).execute()
    print('Returned transferJob: {}'.format(
        json.dumps(result, indent=4)))


if __name__ == '__main__':

    main()
