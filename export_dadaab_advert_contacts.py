import argparse
import csv
import json

from core_data_modules.logging import Logger
from core_data_modules.traced_data.io import TracedDataJsonIO
from id_infrastructure.firestore_uuid_table import FirestoreUuidTable
from storage.google_cloud import google_cloud_utils
from core_data_modules.cleaners import Codes

from src.lib import PipelineConfiguration

log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generates a CSV list of phone numbers from previous"
                                                 " Wusc Dadaab projects")

    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file",
                        help="Path to Dadaab pipeline configuration json file")
    parser.add_argument("messages_traced_data_paths", metavar="messages-traced-data-paths", nargs="+",
                        help="Paths to the messages traced data files to extract phone numbers from")
    parser.add_argument("csv_output_file_path", metavar="csv-output-file-path",
                        help="CSV file path to write the contacts data to")

    args = parser.parse_args()

    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    pipeline_configuration_file_path = args.pipeline_configuration_file_path
    messages_traced_data_paths = args.messages_traced_data_paths
    csv_output_file_path = args.csv_output_file_path

    # Read the settings from the configuration file
    log.info("Loading Pipeline Configuration File...")
    with open(pipeline_configuration_file_path) as f:
        pipeline_configuration = PipelineConfiguration.from_configuration_file(f)
    Logger.set_project_name(pipeline_configuration.pipeline_name)

    log.info("Downloading Firestore UUID Table credentials...")
    firestore_uuid_table_credentials = json.loads(google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path,
        pipeline_configuration.phone_number_uuid_table.firebase_credentials_file_url
    ))

    phone_number_uuid_table = FirestoreUuidTable(
        pipeline_configuration.phone_number_uuid_table.table_name,
        firestore_uuid_table_credentials,
        "avf-phone-uuid-"
    )
    log.info("Initialised the Firestore UUID table")

    advert_uids = set()
    for path in messages_traced_data_paths:
        # Load the traced data
        log.info(f"Loading previous traced data from file '{path}'...")
        with open(path) as f:
            individuals = TracedDataJsonIO.import_jsonl_to_traced_data_iterable(f)
        log.info(f"Loaded {len(individuals)} individuals")

        for ind in individuals:
            if ind["consent_withdrawn"] == Codes.TRUE:
                continue
            advert_uids.add(ind['uid'])

    # Convert the uuids to phone numbers
    log.info("Converting the uuids to phone numbers...")
    uuids_to_phone_numbers = phone_number_uuid_table.uuid_to_data_batch(list(advert_uids))
    advert_contacts = [f"+{uuids_to_phone_numbers[uuid]}" for uuid in advert_uids]

    # Export contacts CSV
    log.warning(f"Exporting {len(advert_contacts)} phone numbers to {csv_output_file_path}...")
    with open(csv_output_file_path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=["URN:Tel", "Name"], lineterminator="\n")
        writer.writeheader()
        for n in advert_contacts:
            writer.writerow({
                "URN:Tel": n
            })
        log.info(f"Wrote {len(advert_contacts)} contacts to {csv_output_file_path}")
