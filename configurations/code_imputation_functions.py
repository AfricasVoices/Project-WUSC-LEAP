import time

from core_data_modules.cleaners import Codes
from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.data_models.code_scheme import CodeTypes
from core_data_modules.traced_data import Metadata


def make_location_code(scheme, clean_value):
    if clean_value == Codes.NOT_CODED:
        return scheme.get_code_with_control_code(Codes.NOT_CODED)
    else:
        return scheme.get_code_with_match_value(clean_value)


def impute_age_category(user, data, age_configurations):
    # TODO: By accepting a list of age_configurations but then requiring that list to contain code schemes in a
    #       certain order, it looks like we're providing more flexibility than we actually do. We should change this
    #       to explicitly accept age and age_category configurations, which requires refactoring all of the
    #       code imputation functions.
    age_cc = age_configurations[0]
    age_category_cc = age_configurations[1]

    age_categories = {
        (10, 14): "10 to 14",
        (15, 17): "15 to 17",
        (18, 35): "18 to 35",
        (36, 54): "36 to 54",
        (55, 99): "55 to 99"
    }

    for td in data:
        age_label = td[age_cc.coded_field]
        age_code = age_cc.code_scheme.get_code_with_code_id(age_label["CodeID"])

        if age_code.code_type == CodeTypes.NORMAL:
            # TODO: If these age categories are standard across projects, move this to Core as a new cleaner.
            age_category = None
            for age_range, category in age_categories.items():
                if age_range[0] <= age_code.numeric_value <= age_range[1]:
                    age_category = category
            assert age_category is not None

            age_category_code = age_category_cc.code_scheme.get_code_with_match_value(age_category)
        elif age_code.code_type == CodeTypes.META:
            age_category_code = age_category_cc.code_scheme.get_code_with_meta_code(age_code.meta_code)
        else:
            assert age_code.code_type == CodeTypes.CONTROL
            age_category_code = age_category_cc.code_scheme.get_code_with_control_code(age_code.control_code)

        age_category_label = CleaningUtils.make_label_from_cleaner_code(
            age_category_cc.code_scheme, age_category_code, Metadata.get_call_location()
        )

        td.append_data(
            {age_category_cc.coded_field: age_category_label.to_dict()},
            Metadata(user, Metadata.get_call_location(), time.time())
        )
