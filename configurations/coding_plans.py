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
               coda_filename="WUSC-LEAP_kalobeyei_s01e01.json",
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
               ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value(
                   "leap kalobeyei s01e01"),
               raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="rqa_s01e02_raw",
               dataset_name="kalobeyei_s01e01",
               listening_group_filename="wusc_leap_kalobeyei_s01e02_listening_group.csv",
               time_field="sent_on",
               run_id_field="rqa_s01e02_run_id",
               coda_filename="WUSC-LEAP_kalobeyei_s01e02.json",
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
               ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value(
                   "leap kalobeyei s01e02"),
               raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="rqa_s01e03_raw",
               dataset_name="kalobeyei_s01e03",
               listening_group_filename="wusc_leap_kalobeyei_s01e03_listening_group.csv",
               time_field="sent_on",
               run_id_field="rqa_s01e03_run_id",
               coda_filename="WUSC-LEAP_kalobeyei_s01e03.json",
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
               ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value(
                   "leap kalobeyei s01e03"),
               raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="rqa_s01e04_raw",
               dataset_name="kalobeyei_s01e04",
               listening_group_filename="wusc_leap_kalobeyei_s01e04_listening_group.csv",
               time_field="sent_on",
               run_id_field="rqa_s01e04_run_id",
               coda_filename="WUSC-LEAP_kalobeyei_s01e04.json",
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
               ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value(
                   "leap kalobeyei s01e04"),
               raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="rqa_s01e05_raw",
               dataset_name="kalobeyei_s01e05",
               listening_group_filename="wusc_leap_kalobeyei_s01e05_listening_group.csv",
               time_field="sent_on",
               run_id_field="rqa_s01e05_run_id",
               coda_filename="WUSC-LEAP_kalobeyei_s01e05.json",
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
               ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value(
                   "leap kalobeyei s01e05"),
               raw_field_fold_strategy=FoldStrategies.concatenate),
  
    CodingPlan( raw_field="s01_lessons_learnt_raw",
                dataset_name="s01_lessons_learnt",
                time_field="sent_on",
                run_id_field="lessons_learnt_run_id",
                coda_filename="WUSC-LEAP_kalobeyei_s01_lessons_learnt.json",
                icr_filename="kalobeyei_s01_lessons_learnt.csv",
                coding_configurations=[
                    CodingConfiguration(
                        coding_mode=CodingModes.MULTIPLE,
                        code_scheme=CodeSchemes.S01_KALOBEYEI_LESSONS_LEARNT,
                        coded_field="s01_lessons_learnt",
                        analysis_file_key="s01_lessons_learnt_",
                        fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                            CodeSchemes.S01_KALOBEYEI_LESSONS_LEARNT, x, y)
                    )
                ],
                ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value(
                    "s01 leap kalobeyei lessons learnt"),
                raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan( raw_field="s01_impact_made_raw",
                dataset_name="s01_impact_made",
                time_field="sent_on",
                run_id_field="impact_made_run_id",
                coda_filename="WUSC-LEAP_kalobeyei_s01_impact_made.json",
                icr_filename="kalobeyei_s01_impact_made.csv",
                coding_configurations=[
                    CodingConfiguration(
                        coding_mode=CodingModes.MULTIPLE,
                        code_scheme=CodeSchemes.S01_KALOBEYEI_IMPACT_MADE,
                        coded_field="s01_impact_made",
                        analysis_file_key="s01_impact_made_",
                        fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                            CodeSchemes.S01_KALOBEYEI_IMPACT_MADE, x, y)
                    )
                ],
                ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value(
                    "s01 leap kalobeyei impact made"),
                raw_field_fold_strategy=FoldStrategies.concatenate)
]

def get_rqa_coding_plans(pipeline_name):
    return KALOBEYEI_S01_RQA_CODING_PLANS


KAKUMA_DEMOG_CODING_PLANS = [
    CodingPlan(raw_field="location_raw",
               dataset_name="kakuma_location",
               time_field="location_time",
               coda_filename="WUSC-KEEP-II_kakuma_location.json",
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
               coda_filename="WUSC-KEEP-II_kakuma_gender.json",
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
               ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value(
                   "kakuma gender"),
               raw_field_fold_strategy=FoldStrategies.assert_equal),

    CodingPlan(raw_field="age_raw",
               dataset_name="kakuma_age",
               time_field="age_time",
               coda_filename="WUSC-KEEP-II_kakuma_age.json",
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
               ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value(
                   "kakuma age"),
               raw_field_fold_strategy=FoldStrategies.assert_equal),

    CodingPlan(raw_field="household_language_raw",
               dataset_name="kakuma_household_language",
               time_field="household_language_time",
               coda_filename="WUSC-KEEP-II_kakuma_household_language.json",
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
               ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value(
                   "kakuma household language"),
               raw_field_fold_strategy=FoldStrategies.assert_equal),

    CodingPlan(raw_field="nationality_raw",
               dataset_name="kakuma_nationality",
               time_field="nationality_time",
               coda_filename="WUSC-KEEP-II_kakuma_nationality.json",
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
               ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value(
                   "kakuma nationality"),
               raw_field_fold_strategy=FoldStrategies.assert_equal),

    CodingPlan(raw_field="kalobeyei_participants_engaging_raw",
               dataset_name="kalobeyei_participants_engaging",
               time_field="kalobeyei_participants_engaging_time",
               coda_filename="WUSC-LEAP_kalobeyei_participants_engaging.json",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.SINGLE,
                       code_scheme=CodeSchemes.KALOBEYEI_PARTICIPANTS_ENGAGING,
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
               coda_filename="WUSC-LEAP_kalobeyei_targeted_group_parent.json",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.SINGLE,
                       code_scheme=CodeSchemes.KALOBEYEI_TARGETED_GROUP_PARENT,
                       coded_field="kalobeyei_targeted_group_parent_coded",
                       analysis_file_key="kalobeyei_targeted_group_parent",
                       fold_strategy=FoldStrategies.assert_label_ids_equal,
                       include_in_theme_distribution=Codes.TRUE
                   )
               ],
               ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value(
                   "leap kalobeyei targeted group parent"),
               raw_field_fold_strategy=FoldStrategies.assert_equal),

    CodingPlan(raw_field="kalobeyei_child_gender_raw",
               dataset_name="kalobeyei_child_gender",
               time_field="kalobeyei_child_gender_time",
               coda_filename="WUSC-LEAP_kalobeyei_child_gender.json",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.SINGLE,
                       code_scheme=CodeSchemes.KALOBEYEI_CHILD_GENDER,
                       coded_field="kalobeyei_child_gender_coded",
                       analysis_file_key="kalobeyei_child_gender",
                       fold_strategy=FoldStrategies.assert_label_ids_equal,
                       include_in_theme_distribution=Codes.TRUE
                   )
               ],
               ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value(
                   "leap kalobeyei child gender"),
               raw_field_fold_strategy=FoldStrategies.assert_equal),

    CodingPlan(raw_field="kalobeyei_consent_to_engage_child_raw",
               dataset_name="kalobeyei_consent_to_engage_child",
               time_field="kalobeyei_consent_to_engage_child_time",
               coda_filename="WUSC-LEAP_kalobeyei_consent_to_engage_child.json",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.SINGLE,
                       code_scheme=CodeSchemes.KALOBEYEI_CONSENT_TO_ENGAGE_CHILD,
                       coded_field="kalobeyei_consent_to_engage_child_coded",
                       analysis_file_key="kalobeyei_consent_to_engage_child",
                       fold_strategy=FoldStrategies.assert_label_ids_equal,
                       include_in_theme_distribution=Codes.TRUE
                   )
               ],
               ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value(
                   "leap kalobeyei consent to engage child"),
               raw_field_fold_strategy=FoldStrategies.assert_equal),

    CodingPlan(raw_field="kalobeyei_age_of_parent_raw",
               dataset_name="kalobeyei_age_of_parent",
               time_field="kalobeyei_age_of_parent_time",
               coda_filename="WUSC-LEAP_kalobeyei_age_of_parent.json",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.SINGLE,
                       code_scheme=CodeSchemes.KALOBEYEI_AGE_OF_PARENT,
                       coded_field="kalobeyei_age_of_parent_coded",
                       analysis_file_key="kalobeyei_age_of_parent",
                       fold_strategy=FoldStrategies.assert_label_ids_equal,
                       include_in_theme_distribution=Codes.TRUE
                   )
               ],
               ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value(
                   "leap kalobeyei age of parent"),
               raw_field_fold_strategy=FoldStrategies.assert_equal),

    CodingPlan(raw_field="kalobeyei_currently_in_school_raw",
               dataset_name="kalobeyei_currently_in_school",
               time_field="kalobeyei_currently_in_school_time",
               coda_filename="WUSC-LEAP_kalobeyei_currently_in_school.json",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.SINGLE,
                       code_scheme=CodeSchemes.KALOBEYEI_CURRENTLY_IN_SCHOOL,
                       coded_field="kalobeyei_currently_in_school_coded",
                       analysis_file_key="kalobeyei_currently_in_school",
                       fold_strategy=FoldStrategies.assert_label_ids_equal,
                       include_in_theme_distribution=Codes.TRUE
                   )
               ],
               ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value(
                   "leap kalobeyei currently in school"),
               raw_field_fold_strategy=FoldStrategies.assert_equal),
]

KALOBEYEI_S01_FOLLOW_UP_CODING_PLANS = [
    CodingPlan(raw_field="kalobeyei_girls_empowerment_raw",
               dataset_name="kalobeyei_girls_empowerment",
               time_field="kalobeyei_girls_empowerment_time",
               coda_filename="WUSC-LEAP_kalobeyei_girls_empowerment.json",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.MULTIPLE,
                       code_scheme=CodeSchemes.KALOBEYEI_GIRLS_EMPOWERMENT,
                       coded_field="kalobeyei_girls_empowerment_coded",
                       analysis_file_key="kalobeyei_girls_empowerment",
                       fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                           CodeSchemes.KALOBEYEI_GIRLS_EMPOWERMENT, x, y)
                   )
               ],
               ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value(
                   "leap kalobeyei girls empowerment"),
               raw_field_fold_strategy=FoldStrategies.concatenate),
]


def get_demog_coding_plans(pipeline_name):
    return KAKUMA_DEMOG_CODING_PLANS

def get_ws_correct_dataset_scheme(pipeline_name):
    return CodeSchemes.KAKUMA_WS_CORRECT_DATASET

def get_follow_up_coding_plans(pipeline_name):
    return KALOBEYEI_S01_FOLLOW_UP_CODING_PLANS
