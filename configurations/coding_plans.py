from core_data_modules.cleaners import somali, swahili, Codes
from core_data_modules.traced_data.util.fold_traced_data import FoldStrategies

from configurations import code_imputation_functions
from configurations.code_schemes import CodeSchemes
from src.lib.configuration_objects import CodingConfiguration, CodingModes, CodingPlan


def clean_age_with_range_filter(text):
    """
    Cleans age from the given `text`, setting to NC if the cleaned age is not in the range 10 <= age < 100.
    """
    age = swahili.DemographicCleaner.clean_age(text)
    if type(age) == int and 10 <= age < 100:
        return str(age)
        # TODO: Once the cleaners are updated to not return Codes.NOT_CODED, this should be updated to still return
        #       NC in the case where age is an int but is out of range
    else:
        return Codes.NOT_CODED


KALOBEYEI_S01_RQA_CODING_PLANS = [
            CodingPlan(raw_field="rqa_s01e01_raw",
                       dataset_name="kalobeyei_s01e01",
                       listening_group_filename="wusc_leap_kalobeyei_s01e01_listening_group.csv",
                       time_field="sent_on",
                       run_id_field="rqa_s01e01_run_id",
                       coda_filename="kalobeyei_s01e01.json",
                       icr_filename="s01e01.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.KALOBEYEI_S01E01,
                               coded_field="rqa_s01e01_coded",
                               analysis_file_key="rqa_s01e01_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.KALOBEYEI_S01E01, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("leap kalobeyei s01e01"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

CodingPlan(raw_field="rqa_s01e02_raw",
                       dataset_name="kalobeyei_s01e01",
                       listening_group_filename="wusc_leap_kalobeyei_s01e02_listening_group.csv",
                       time_field="sent_on",
                       run_id_field="rqa_s01e02_run_id",
                       coda_filename="kalobeyei_s01e02.json",
                       icr_filename="s01e02.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.KALOBEYEI_S01E02,
                               coded_field="rqa_s01e02_coded",
                               analysis_file_key="rqa_s01e02_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.KALOBEYEI_S01E02, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("leap kalobeyei s01e02"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

CodingPlan(raw_field="rqa_s01e03_raw",
                       dataset_name="kalobeyei_s01e03",
                       listening_group_filename="wusc_leap_kalobeyei_s01e03_listening_group.csv",
                       time_field="sent_on",
                       run_id_field="rqa_s01e03_run_id",
                       coda_filename="kalobeyei_s01e03.json",
                       icr_filename="s01e03.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.KALOBEYEI_S01E03,
                               coded_field="rqa_s01e03_coded",
                               analysis_file_key="rqa_s01e03_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.KALOBEYEI_S01E03, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("leap kalobeyei s01e03"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

CodingPlan(raw_field="rqa_s01e04_raw",
                       dataset_name="kalobeyei_s01e04",
                       listening_group_filename="wusc_leap_kalobeyei_s01e04_listening_group.csv",
                       time_field="sent_on",
                       run_id_field="rqa_s01e04_run_id",
                       coda_filename="kalobeyei_s01e04.json",
                       icr_filename="s01e04.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.KALOBEYEI_S01E04,
                               coded_field="rqa_s01e04_coded",
                               analysis_file_key="rqa_s01e04_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.KALOBEYEI_S01E04, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("leap kalobeyei s01e04"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

CodingPlan(raw_field="rqa_s01e05_raw",
                       dataset_name="kalobeyei_s01e05",
                       listening_group_filename="wusc_leap_kalobeyei_s01e05_listening_group.csv",
                       time_field="sent_on",
                       run_id_field="rqa_s01e05_run_id",
                       coda_filename="kalobeyei_s01e05.json",
                       icr_filename="s01e05.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.KALOBEYEI_S01E05,
                               coded_field="rqa_s01e05_coded",
                               analysis_file_key="rqa_s01e05_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.KALOBEYEI_S01E05, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("leap kalobeyei s01e05"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),
    ]

def get_rqa_coding_plans(pipeline_name):
        return KALOBEYEI_S01_RQA_CODING_PLANS

KAKUMA_DEMOG_CODING_PLANS = [
        CodingPlan(raw_field="location_raw",
                   dataset_name="kakuma_location",
                   time_field="location_time",
                   coda_filename="kakuma_location.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.KAKUMA_LOCATION,
                           coded_field="location_coded",
                           analysis_file_key="location",
                           fold_strategy=FoldStrategies.assert_label_ids_equal,
                           include_in_theme_distribution=Codes.TRUE
                       ),
                   ],
                   ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma location"),
                   raw_field_fold_strategy=FoldStrategies.assert_equal),

        CodingPlan(raw_field="gender_raw",
                   dataset_name="kakuma_gender",
                   time_field="gender_time",
                   coda_filename="kakuma_gender.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.GENDER,
                           cleaner=somali.DemographicCleaner.clean_gender,
                           coded_field="gender_coded",
                           analysis_file_key="gender",
                           fold_strategy=FoldStrategies.assert_label_ids_equal,
                           include_in_theme_distribution=Codes.TRUE
                       )
                   ],
                   ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma gender"),
                   raw_field_fold_strategy=FoldStrategies.assert_equal),

        CodingPlan(raw_field="age_raw",
                   dataset_name="kakuma_age",
                   time_field="age_time",
                   coda_filename="kakuma_age.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.AGE,
                           cleaner=lambda text: clean_age_with_range_filter(text),
                           coded_field="age_coded",
                           analysis_file_key="age",
                           fold_strategy=FoldStrategies.assert_label_ids_equal,
                           include_in_theme_distribution=Codes.FALSE
                       ),
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.AGE_CATEGORY,
                           coded_field="age_category_coded",
                           analysis_file_key="age_category",
                           fold_strategy=FoldStrategies.assert_label_ids_equal,
                           include_in_theme_distribution=Codes.TRUE
                       )
                   ],
                   code_imputation_function=code_imputation_functions.impute_age_category,
                   ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma age"),
                   raw_field_fold_strategy=FoldStrategies.assert_equal),

        CodingPlan(raw_field="household_language_raw",
                   dataset_name="kakuma_household_language",
                   time_field="household_language_time",
                   coda_filename="kakuma_household_language.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.KAKUMA_HOUSEHOLD_LANGUAGE,
                           coded_field="household_language_coded",
                           analysis_file_key="household_language",
                           fold_strategy=FoldStrategies.assert_label_ids_equal,
                           include_in_theme_distribution=Codes.TRUE
                       )
                   ],
                   ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma household language"),
                   raw_field_fold_strategy=FoldStrategies.assert_equal),

        CodingPlan(raw_field="nationality_raw",
                   dataset_name="kakuma_nationality",
                   time_field="nationality_time",
                   coda_filename="kakuma_nationality.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.NATIONALITY,
                           coded_field="nationality_coded",
                           analysis_file_key="nationality",
                           fold_strategy=FoldStrategies.assert_label_ids_equal,
                           include_in_theme_distribution=Codes.TRUE
                       )
                   ],
                   ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma nationality"),
                   raw_field_fold_strategy=FoldStrategies.assert_equal),

        CodingPlan(raw_field="participants_engaging_raw",
                   dataset_name="kalobeyei_participants_engaging",
                   time_field="kalobeyei_participants_engaging_time",
                   coda_filename="kalobeyei_participants_engaging.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.NATIONALITY,
                           coded_field="kalobeyei_participants_engaging_coded",
                           analysis_file_key="kalobeyei_participants_engaging",
                           fold_strategy=FoldStrategies.assert_label_ids_equal,
                           include_in_theme_distribution=Codes.TRUE
                       )
                   ],
                   ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("leap kalobeyei participants engaging"),
           raw_field_fold_strategy=FoldStrategies.assert_equal),

        CodingPlan(raw_field="kalobeyei_targeted_group_parent_raw",
                   dataset_name="kalobeyei_targeted_group_parent",
                   time_field="kalobeyei_targeted_group_parent_time",
                   coda_filename="kalobeyei_targeted_group_parent.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.NATIONALITY,
                           coded_field="kalobeyei_targeted_group_parent_coded",
                           analysis_file_key="kalobeyei_targeted_group_parent",
                           fold_strategy=FoldStrategies.assert_label_ids_equal,
                           include_in_theme_distribution=Codes.TRUE
                       )
                   ],
                   ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("leap kalobeyei targeted group parent"),
           raw_field_fold_strategy=FoldStrategies.assert_equal),

        CodingPlan(raw_field="kalobeyei_child_gender_raw",
                   dataset_name="kalobeyei_child_gender",
                   time_field="kalobeyei_child_gender_time",
                   coda_filename="kalobeyei_child_gender.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.NATIONALITY,
                           coded_field="kalobeyei_child_gender_coded",
                           analysis_file_key="kalobeyei_child_gender",
                           fold_strategy=FoldStrategies.assert_label_ids_equal,
                           include_in_theme_distribution=Codes.TRUE
                       )
                   ],
                   ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("leap kalobeyei child gender"),
   raw_field_fold_strategy=FoldStrategies.assert_equal),

        CodingPlan(raw_field="kalobeyei_consent_to_engage_child_raw",
                                           dataset_name="kalobeyei_consent_to_engage_child",
                                           time_field="kalobeyei_consent_to_engage_child_time",
                                           coda_filename="kalobeyei_consent_to_engage_child.json",
                                           coding_configurations=[
                                               CodingConfiguration(
                                                   coding_mode=CodingModes.SINGLE,
                                                   code_scheme=CodeSchemes.NATIONALITY,
                                                   coded_field="kalobeyei_consent_to_engage_child_coded",
                                                   analysis_file_key="kalobeyei_consent_to_engage_child",
                                                   fold_strategy=FoldStrategies.assert_label_ids_equal,
                                                   include_in_theme_distribution=Codes.TRUE
                                               )
                                           ],
                                           ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("leap kalobeyei consent to engage child"),
                           raw_field_fold_strategy=FoldStrategies.assert_equal),

        CodingPlan(raw_field="kalobeyei_age_of_parent_raw",
                                                           dataset_name="kalobeyei_age_of_parent",
                                                           time_field="kalobeyei_age_of_parent_time",
                                                           coda_filename="kalobeyei_age_of_parent.json",
                                                           coding_configurations=[
                                                               CodingConfiguration(
                                                                   coding_mode=CodingModes.SINGLE,
                                                                   code_scheme=CodeSchemes.NATIONALITY,
                                                                   coded_field="kalobeyei_age_of_parent_coded",
                                                                   analysis_file_key="kalobeyei_age_of_parent",
                                                                   fold_strategy=FoldStrategies.assert_label_ids_equal,
                                                                   include_in_theme_distribution=Codes.TRUE
                                                               )
                                                           ],
                                                           ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("leap kalobeyei age of parent"),
                                           raw_field_fold_strategy=FoldStrategies.assert_equal),

        CodingPlan(raw_field="kalobeyei_currently_in_school_raw",
                                                                   dataset_name="kalobeyei_currently_in_school",
                                                                   time_field="kalobeyei_currently_in_school_time",
                                                                   coda_filename="kalobeyei_currently_in_school.json",
                                                                   coding_configurations=[
                                                                       CodingConfiguration(
                                                                           coding_mode=CodingModes.SINGLE,
                                                                           code_scheme=CodeSchemes.NATIONALITY,
                                                                           coded_field="kalobeyei_currently_in_school_coded",
                                                                           analysis_file_key="kalobeyei_currently_in_school",
                                                                           fold_strategy=FoldStrategies.assert_label_ids_equal,
                                                                           include_in_theme_distribution=Codes.TRUE
                                                                       )
                                                                   ],
                                                                   ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("leap kalobeyei currently in school"),
                                                   raw_field_fold_strategy=FoldStrategies.assert_equal),

        ]

KALOBEYEI_S01_FOLLOW_UP_CODING_PLANS = [

]

def get_demog_coding_plans(pipeline_name):
        return KAKUMA_DEMOG_CODING_PLANS

def get_ws_correct_dataset_scheme(pipeline_name):
        return CodeSchemes.KAKUMA_WS_CORRECT_DATASET

def get_follow_up_coding_plans(pipeline_name):
    return KALOBEYEI_S01_FOLLOW_UP_CODING_PLANS
