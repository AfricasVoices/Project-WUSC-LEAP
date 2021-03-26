import json

from core_data_modules.data_models import CodeScheme


def _open_scheme(filename):
    with open(f"code_schemes/{filename}", "r") as f:
        firebase_map = json.load(f)
        return CodeScheme.from_firebase_map(firebase_map)


class CodeSchemes(object):

    KALOBEYEI_S01E01 = _open_scheme("kalobeyei_s01e01.json")
    KALOBEYEI_S01E02 = _open_scheme("kalobeyei_s01e02.json")
    KALOBEYEI_S01E03 = _open_scheme("kalobeyei_s01e03.json")
    KALOBEYEI_S01E04 = _open_scheme("kalobeyei_s01e04.json")
    KALOBEYEI_S01E05 = _open_scheme("kalobeyei_s01e05.json")

    GENDER = _open_scheme("gender.json")
    NATIONALITY = _open_scheme("nationality.json")
    AGE = _open_scheme("age.json")
    AGE_CATEGORY = _open_scheme("age_category.json")
    KAKUMA_HOUSEHOLD_LANGUAGE = _open_scheme("kakuma_household_language.json")
    KAKUMA_LOCATION = _open_scheme("kakuma_location.json")

    KALOBEYEI_PARTICIPANTS_ENGAGING = _open_scheme("kalobeyei_participants_engaging.json")
    KALOBEYEI_TARGETED_GROUP_PARENT = _open_scheme("kalobeyei_targeted_group_parent.json")
    KALOBEYEI_CHILD_GENDER = _open_scheme("kalobeyei_child_gender.json")
    KALOBEYEI_CONSENT_TO_ENGAGE_CHILD = _open_scheme("kalobeyei_consent_to_engage_child.json")
    KALOBEYEI_AGE_OF_PARENT = _open_scheme("kalobeyei_age_of_parent.json")
    KALOBEYEI_CURRENTLY_IN_SCHOOL = _open_scheme("kalobeyei_currently_in_school.json")
    KALOBEYEI_GIRLS_EMPOWERMENT = _open_scheme("kalobeyei_girls_empowerment.json")
    S01_KALOBEYEI_LESSONS_LEARNT = _open_scheme("s01_kalobeyei_lessons_learnt.json")
    S01_KALOBEYEI_IMPACT_MADE = _open_scheme("s01_kalobeyei_impact_made.json")

    KAKUMA_WS_CORRECT_DATASET = _open_scheme("kakuma_ws_correct_dataset.json")
