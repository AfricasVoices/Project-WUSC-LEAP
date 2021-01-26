import json

from core_data_modules.data_models import CodeScheme


def _open_scheme(filename):
    with open(f"code_schemes/{filename}", "r") as f:
        firebase_map = json.load(f)
        return CodeScheme.from_firebase_map(firebase_map)


class CodeSchemes(object):

    S01E01 = _open_scheme("s01e01.json")
    S01E02 = _open_scheme("s01e02.json")
    S01E03 = _open_scheme("s01e03.json")
    S01E04 = _open_scheme("s01e04.json")
    S01E05 = _open_scheme("s01e05.json")

    GENDER = _open_scheme("gender.json")
    NATIONALITY = _open_scheme("nationality.json")
    AGE = _open_scheme("age.json")
    AGE_CATEGORY = _open_scheme("age_category.json")
    KAKUMA_HOUSEHOLD_LANGUAGE = _open_scheme("kakuma_household_language.json")
    KAKUMA_LOCATION = _open_scheme("kakuma_location.json")

    KAKUMA_WS_CORRECT_DATASET = _open_scheme("kakuma_ws_correct_dataset.json")
