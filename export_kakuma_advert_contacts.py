import argparse
import csv
import json
from collections import OrderedDict

from core_data_modules.logging import Logger
from core_data_modules.cleaners import Codes
from core_data_modules.traced_data.io import TracedDataJsonIO
from id_infrastructure.firestore_uuid_table import FirestoreUuidTable
from storage.google_cloud import google_cloud_utils

from src.lib import PipelineConfiguration
from configurations.code_schemes import CodeSchemes

Logger.set_project_name("WUSC-KEEP-II")
log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generates a CSV of phone numbers and their  repective Textit ISO 639 "
                                                 "language code based on their manually labeled demographic response")

    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file",
                        help="Path to WUSC-KEEP-II-KAKUMA pipeline configuration json file")
    parser.add_argument("listening_group_data_dir", metavar="listening-group-data-dir",
                        help="Directory path to read listening group CSV files to extract listening group data from,")
    parser.add_argument("messages_traced_data_paths", metavar="messages-traced-data-paths", nargs="+",
                        help="Paths to the messages traced data files to extract phone numbers from")
    parser.add_argument("contacts_csv_path", metavar="contacts-csv-path",
                        help="CSV file path to write the contacts data to")

    args = parser.parse_args()

    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    pipeline_configuration_file_path = args.pipeline_configuration_file_path
    listening_group_data_dir = args.listening_group_data_dir
    messages_traced_data_paths = args.messages_traced_data_paths
    contacts_csv_path = args.contacts_csv_path

    # Read the settings from the configuration file
    log.info("Loading Pipeline Configuration File...")
    with open(pipeline_configuration_file_path) as f:
        pipeline_configuration = PipelineConfiguration.from_configuration_file(f)
        assert pipeline_configuration.pipeline_name in ["kakuma_s01_pipeline", "kakuma_s02_pipeline", "kakuma_all_seasons_pipeline"], \
            "PipelineName must be either a 'seasonal pipeline' or 'all seasons pipeline'"

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


    # Search the Messages TracedData and listening group CSV files for uuids for kakuma participants based on their
    # manually labelled household language i.e Oromo/Sudanese-Juba-arabic/Somali/English/Swahili speakers.
    oromo_uuids = set()
    sudanese_juba_arabic_uuids = set()
    turkana_uuids = set()
    somali_uuids = set()
    english_uuids = set()
    swahili_uuids = set()
    all_uuids = set()

    for path in messages_traced_data_paths:
        # Load the traced data
        log.info(f"Loading previous traced data from file '{path}'...")
        with open(path) as f:
            messages = TracedDataJsonIO.import_jsonl_to_traced_data_iterable(f)
        log.info(f"Loaded {len(messages)} traced data objects")

        log.info(f'Searching for the participants uuids vis-a-vis` their manually labelled '
                 f'demographic language response')

        for msg in messages:
            if msg['uid'] in all_uuids or msg["consent_withdrawn"] == Codes.TRUE:
                continue

            all_uuids.add(msg['uid'])

            if CodeSchemes.KAKUMA_HOUSEHOLD_LANGUAGE.get_code_with_code_id(
                    msg["household_language_coded"]["CodeID"]).string_value == "oromo":
                oromo_uuids.add(msg['uid'])
            elif CodeSchemes.KAKUMA_HOUSEHOLD_LANGUAGE.get_code_with_code_id(
                        msg["household_language_coded"]["CodeID"]).string_value == "sudanese":
                    sudanese_juba_arabic_uuids.add(msg['uid'])
            elif CodeSchemes.KAKUMA_HOUSEHOLD_LANGUAGE.get_code_with_code_id(
                        msg["household_language_coded"]["CodeID"]).string_value == "turkana":
                    turkana_uuids.add(msg['uid'])
            elif CodeSchemes.KAKUMA_HOUSEHOLD_LANGUAGE.get_code_with_code_id(
                        msg["household_language_coded"]["CodeID"]).string_value == "somali":
                    somali_uuids.add(msg['uid'])
            elif CodeSchemes.KAKUMA_HOUSEHOLD_LANGUAGE.get_code_with_code_id(
                        msg["household_language_coded"]["CodeID"]).string_value == "english":
                    english_uuids.add(msg['uid'])
            else:
                swahili_uuids.add(msg['uid'])

    # Load all listening group de-identified CSV files
    listening_group_csvs = []
    for listening_group_csv_url in pipeline_configuration.listening_group_csv_urls:
        listening_group_csvs.append(listening_group_csv_url.split("/")[-1])

    for listening_group_csv in listening_group_csvs:
        with open(f'{listening_group_data_dir}/Raw Data/{listening_group_csv}', "r", encoding='utf-8-sig') as f:
            data = list(csv.DictReader(f))
            log.info(
                f'Loaded {len(data)} listening group participants from {listening_group_data_dir}/Raw Data/{listening_group_csv}')

            # Add the lg avf-phone-uuids to their respective language set
            for row in data:
                if row['avf-phone-uuid'] in all_uuids: # because we are giving precedence to the participants manually
                                                       # labeled language
                    continue

                all_uuids.add(row['avf-phone-uuid'])

                if row['Language'] == 'orm':
                    oromo_uuids.add(row['avf-phone-uuid'])
                elif row['Language'] == 'apd':
                    sudanese_juba_arabic_uuids.add(row['avf-phone-uuid'])
                elif row['Language'] == 'tuv':
                    turkana_uuids.add(row['avf-phone-uuid'])
                elif row['Language'] == 'som':
                    somali_uuids.add(row['avf-phone-uuid'])
                elif row['Language'] == 'eng':
                    english_uuids.add(row['avf-phone-uuid'])
                else:
                    swahili_uuids.add(row['avf-phone-uuid'])

    # Convert the uuids to phone numbers
    log.info("Converting the uuids to phone numbers...")
    uuids_to_phone_numbers = phone_number_uuid_table.uuid_to_data_batch(list(all_uuids))

    oromo_phone_numbers = [f"+{uuids_to_phone_numbers[uuid]}" for uuid in oromo_uuids]
    sudanese_phone_numbers = [f"+{uuids_to_phone_numbers[uuid]}" for uuid in sudanese_juba_arabic_uuids]
    turkana_phone_numbers = [f"+{uuids_to_phone_numbers[uuid]}" for uuid in turkana_uuids]
    somali_phone_numbers = [f"+{uuids_to_phone_numbers[uuid]}" for uuid in somali_uuids]
    english_phone_numbers = [f"+{uuids_to_phone_numbers[uuid]}" for uuid in english_uuids]
    swahili_phone_numbers = [f"+{uuids_to_phone_numbers[uuid]}" for uuid in swahili_uuids]

    # Export the phone number and language pairs to a CSV
    # TODO upload this to keep_ii_kakuma textit instance through API?
    advert_contacts = OrderedDict()
    for phone_number in oromo_phone_numbers:
        advert_contacts[phone_number] = {
            "URN:Tel": phone_number,
            "Name": None,
            "Language": 'orm'
        }
    for phone_number in sudanese_phone_numbers:
        advert_contacts[phone_number] = {
            "URN:Tel": phone_number,
            "Name": None,
            "Language": 'apd'
        }
    for phone_number in turkana_phone_numbers:
        advert_contacts[phone_number] = {
            "URN:Tel": phone_number,
            "Name": None,
            "Language": 'tuv'
        }
    for phone_number in somali_phone_numbers:
        advert_contacts[phone_number] = {
            "URN:Tel": phone_number,
            "Name": None,
            "Language": 'som'
        }
    for phone_number in english_phone_numbers:
        advert_contacts[phone_number] = {
            "URN:Tel": phone_number,
            "Name": None,
            "Language": 'eng'
        }
    for phone_number in swahili_phone_numbers:
        advert_contacts[phone_number] = {
            "URN:Tel": phone_number,
            "Name": None,
            "Language": 'swh'
        }

    log.warning(f"Exporting {len(advert_contacts)} contacts to {contacts_csv_path}")
    with open(contacts_csv_path, "w") as f:
        headers = ["URN:Tel", "Name", "Language"]
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()
        for phone_number in advert_contacts.values():
            writer.writerow(phone_number)

        log.info(f"Wrote {len(advert_contacts)} contacts to {contacts_csv_path}")
