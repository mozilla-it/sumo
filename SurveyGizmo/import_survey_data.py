#!/usr/bin/env python3
import logging
import os
import csv
import argparse
import datetime
from dateutil import parser as dateparser

from fs import open_fs
from surveygizmo import SurveyGizmo, surveygizmo

# Evil, but silence an annoying warning please
import google.auth._default
google.auth._default._warn_about_problematic_credentials = lambda *a, **k: None

logging.basicConfig(
    format=
    "[%(asctime)s] %(levelname)s [%(filename)s.%(funcName)s:%(lineno)d] %(message)s",
    level=logging.INFO)

logger = logging.getLogger(__name__)

try:
    import google.cloud.logging
    # Instantiates a client
    client = google.cloud.logging.Client()
    client.setup_logging()
except:
    pass

SURVEYGIZMO_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Defaults
SURVEY_ID = 4669267
RESULTS_PER_PAGE = None
OUTPUT_BUCKET = 'gs://moz-it-data-sumo'
OUTPUT_FOLDER = 'surveygizmo'
OUTPUT_FILE = 'csat_results_new.csv'

OUTPUT_FIELDS = [
    "Response ID",
    "Time Started",
    "Date Submitted",
    "Status",
    "Contact ID",
    "Language",  #"Legacy Comments","Comments",
    "Referer",
    "SessionID",
    "User Agent",  #"Extended Referer",
    "Longitude",  #
    "Latitude",
    "Country",
    "City",
    "State/Region",
    "Postal",
    "Did you accomplish the goal of your visit?",  #2
    "How would you rate your experience with support.mozilla.org (Please help us by only rating the website and not Firefox)",  #4
]

SURVEY_GIZMO_FIELDS = [
    'id',
    'date_started',
    'date_submitted',
    'status',
    'contact_id',
    'language',
    'referer',
    'session_id',
    'user_agent',
    'longitude',
    'latitude',
    'country',
    'city',
    'region',
    'postal',
]

SURVEY_GIZMO_QUESTIONS = [
    2,
    4,
]


def main(survey_id, bucket, output_folder, output_file, results_per_page,
         start, end, **kwargs):
    logger.info("Writing to %s/%s/%s", bucket, output_folder, output_file)
    client = SurveyGizmo(
        api_version='v5',
        api_token=os.getenv("SUMO_SURVEYGIZMO_TOKEN"),
        api_token_secret=os.getenv("SUMO_SURVEYGIZMO_KEY"),
        handler52x=surveygizmo.default_52xhandler,
    )

    # Prepare our output to GCS
    gcsfs = open_fs(bucket + "/?strict=False")
    gcsfs.makedirs(output_folder, recreate=True)
    output = gcsfs.open(output_folder + "/" + output_file, "w")

    # Prepare our CSV writer into GCS
    writer = csv.writer(output,
                        delimiter=',',
                        quoting=csv.QUOTE_ALL,
                        skipinitialspace=True)

    writer.writerow(OUTPUT_FIELDS)

    start_date = start.strftime(SURVEYGIZMO_DATE_FORMAT)
    end_date = end.strftime(SURVEYGIZMO_DATE_FORMAT)

    logger.info("Extrating from %s to %s", start_date, end_date)

    # Prepare our filtered query
    query = client.api.surveyresponse
    query = query.filter('date_submitted', '>=', start_date)
    query = query.filter('date_submitted', '<=', end_date)

    page = 1
    total_pages = 0
    result_ok = True
    total_responses = 0

    while result_ok:
        responses = query.list(survey_id,
                               page=page,
                               resultsperpage=results_per_page)

        result_ok = responses['result_ok']
        total_pages = responses['total_pages']

        for response in responses['data']:
            survey_data = response['survey_data']

            response_data = [response[k] for k in SURVEY_GIZMO_FIELDS]
            questions_data = [
                survey_data[str(q)].get('answer', '')
                for q in SURVEY_GIZMO_QUESTIONS
            ]

            writer.writerow(response_data + questions_data)

            total_responses += 1
            # Dump out some progress
            if total_responses % int(responses['results_per_page']) == 0:
                logger.info("[p:%.3d/%.3d] Total responses: %d/%d", page,
                            total_pages, total_responses,
                            responses['total_count'])

        # Go to the next page or we are done
        page = page + 1
        if page > total_pages:
            break

    logger.info("Processed a total of %d/%r responses", total_responses,
                responses['total_count'])


def valid_date(date):
    try:
        return dateparser.parse(date)
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(date)
        raise argparse.ArgumentTypeError(msg)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="SUMO Survey Gizmo main arguments")
    parser.add_argument('--bucket',
                        type=str,
                        default=OUTPUT_BUCKET,
                        help="URI of the gcs bucket to write output to")
    parser.add_argument('--output_file',
                        type=str,
                        default=OUTPUT_FILE,
                        help="output filename")
    parser.add_argument('--output_folder',
                        type=str,
                        default=OUTPUT_FOLDER,
                        help="output folder")
    parser.add_argument('--survey_id',
                        type=int,
                        default=SURVEY_ID,
                        help="SurveyGizmo survey id to extract")
    parser.add_argument('--results-per-page',
                        dest='results_per_page',
                        type=int,
                        default=RESULTS_PER_PAGE,
                        help="How many results per SurveyGizmo request")

    parser.add_argument('--start',
                        required=False,
                        default=None,
                        type=valid_date,
                        help="When to start looking for anwsers")
    parser.add_argument('--end',
                        required=False,
                        default=datetime.datetime.now(),
                        type=valid_date,
                        help="When to stop looking for answers")

    #parser.add_argument('--outdir', nargs='?', const='.', type=str, help='file output directory')
    args = parser.parse_args()

    if args.start is None:
        logger.info("Defaulting to looking back 24 hours")
        args.start = args.end - datetime.timedelta(days=1)

    if args.start > args.end:
        logger.error("Start %s is after end %s", args.start, args.end)
        exit(1)

    options = vars(args)
    main(**options)
