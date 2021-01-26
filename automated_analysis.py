import argparse
import csv
from collections import OrderedDict
import sys

from core_data_modules.cleaners import Codes
from core_data_modules.data_models.code_scheme import CodeTypes
from core_data_modules.logging import Logger
from core_data_modules.traced_data.io import TracedDataJsonIO
from core_data_modules.util import IOUtils

from src.lib import PipelineConfiguration
from src.lib.configuration_objects import CodingModes
from src import AnalysisUtils

log = Logger(__name__)

IMG_SCALE_FACTOR = 10  # Increase this to increase the resolution of the outputted PNGs
CONSENT_WITHDRAWN_KEY = "consent_withdrawn"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Runs automated analysis over the outputs produced by "
                                                 "`generate_outputs.py`, and optionally uploads the outputs to Drive.")

    parser.add_argument("user", help="User launching this program")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file",
                        help="Path to the pipeline configuration json file")

    parser.add_argument("messages_json_input_path", metavar="messages-json-input-path",
                        help="Path to a JSONL file to read the TracedData of the messages data from")
    parser.add_argument("individuals_json_input_path", metavar="individuals-json-input-path",
                        help="Path to a JSONL file to read the TracedData of the messages data from")
    parser.add_argument("automated_analysis_output_dir", metavar="automated-analysis-output-dir",
                        help="Directory to write the automated analysis outputs to")

    args = parser.parse_args()

    user = args.user
    pipeline_configuration_file_path = args.pipeline_configuration_file_path

    messages_json_input_path = args.messages_json_input_path
    individuals_json_input_path = args.individuals_json_input_path
    automated_analysis_output_dir = args.automated_analysis_output_dir

    IOUtils.ensure_dirs_exist(automated_analysis_output_dir)
    IOUtils.ensure_dirs_exist(f"{automated_analysis_output_dir}/graphs")

    log.info("Loading Pipeline Configuration File...")
    with open(pipeline_configuration_file_path) as f:
        pipeline_configuration = PipelineConfiguration.from_configuration_file(f)
    Logger.set_project_name(pipeline_configuration.pipeline_name)
    log.debug(f"Pipeline name is {pipeline_configuration.pipeline_name}")

    sys.setrecursionlimit(30000)
    # Read the messages dataset
    log.info(f"Loading the messages dataset from {messages_json_input_path}...")
    with open(messages_json_input_path) as f:
        messages = TracedDataJsonIO.import_jsonl_to_traced_data_iterable(f)
        for i in range (len(messages)):
            messages[i] = dict(messages[i].items())
    log.info(f"Loaded {len(messages)} messages")

    # Read the individuals dataset
    log.info(f"Loading the individuals dataset from {individuals_json_input_path}...")
    with open(individuals_json_input_path) as f:
        individuals = TracedDataJsonIO.import_jsonl_to_traced_data_iterable(f)
        for i in range (len(individuals)):
            individuals[i] = dict(individuals[i].items())
    log.info(f"Loaded {len(individuals)} individuals")

    # Compute the number of messages, individuals, and relevant messages per episode and overall.
    log.info("Computing the per-episode and per-season engagement counts...")
    engagement_counts = OrderedDict()  # of episode name to counts
    for plan in PipelineConfiguration.RQA_CODING_PLANS:
        engagement_counts[plan.dataset_name] = {
            "Episode": plan.dataset_name,

            "Total Messages": "-",  # Can't report this for individual weeks because the data has been overwritten with "STOP"
            "Total Messages with Opt-Ins": len(AnalysisUtils.filter_opt_ins(messages, CONSENT_WITHDRAWN_KEY, [plan])),
            "Total Labelled Messages": len(AnalysisUtils.filter_fully_labelled(messages, CONSENT_WITHDRAWN_KEY, [plan])),
            "Total Relevant Messages": len(AnalysisUtils.filter_relevant(messages, CONSENT_WITHDRAWN_KEY, [plan])),

            "Total Participants": "-",
            "Total Participants with Opt-Ins": len(AnalysisUtils.filter_opt_ins(individuals, CONSENT_WITHDRAWN_KEY, [plan])),
            "Total Relevant Participants": len(AnalysisUtils.filter_relevant(individuals, CONSENT_WITHDRAWN_KEY, [plan]))
        }
    engagement_counts["Total"] = {
        "Episode": "Total",

        "Total Messages": len(messages),
        "Total Messages with Opt-Ins": len(AnalysisUtils.filter_opt_ins(messages, CONSENT_WITHDRAWN_KEY, PipelineConfiguration.RQA_CODING_PLANS)),
        "Total Labelled Messages": len(AnalysisUtils.filter_partially_labelled(messages, CONSENT_WITHDRAWN_KEY, PipelineConfiguration.RQA_CODING_PLANS)),
        "Total Relevant Messages": len(AnalysisUtils.filter_relevant(messages, CONSENT_WITHDRAWN_KEY, PipelineConfiguration.RQA_CODING_PLANS)),

        "Total Participants": len(individuals),
        "Total Participants with Opt-Ins": len(AnalysisUtils.filter_opt_ins(individuals, CONSENT_WITHDRAWN_KEY, PipelineConfiguration.RQA_CODING_PLANS)),
        "Total Relevant Participants": len(AnalysisUtils.filter_relevant(individuals, CONSENT_WITHDRAWN_KEY, PipelineConfiguration.RQA_CODING_PLANS))
    }

    with open(f"{automated_analysis_output_dir}/engagement_counts.csv", "w") as f:
        headers = [
            "Episode",
            "Total Messages", "Total Messages with Opt-Ins", "Total Labelled Messages", "Total Relevant Messages",
            "Total Participants", "Total Participants with Opt-Ins", "Total Relevant Participants"
        ]
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()

        for row in engagement_counts.values():
            writer.writerow(row)

    log.info("Computing the participation frequencies...")
    repeat_participations = OrderedDict()
    for i in range(1, len(PipelineConfiguration.RQA_CODING_PLANS) + 1):
        repeat_participations[i] = {
            "Number of Episodes Participated In": i,
            "Number of Participants with Opt-Ins": 0,
            "% of Participants with Opt-Ins": None
        }

    # Compute the number of individuals who participated each possible number of times, from 1 to <number of RQAs>
    # An individual is considered to have participated if they sent a message and didn't opt-out, regardless of the
    # relevance of any of their messages.
    for ind in individuals:
        if AnalysisUtils.withdrew_consent(ind, CONSENT_WITHDRAWN_KEY):
            continue

        weeks_participated = 0
        for plan in PipelineConfiguration.RQA_CODING_PLANS:
            if AnalysisUtils.opt_in(ind, CONSENT_WITHDRAWN_KEY, plan):
                weeks_participated += 1
        assert weeks_participated != 0, f"Found individual '{ind['uid']}' with no participation in any week"
        repeat_participations[weeks_participated]["Number of Participants with Opt-Ins"] += 1

    # Compute the percentage of individuals who participated each possible number of times.
    # Percentages are computed out of the total number of participants who opted-in.
    total_participants = len(AnalysisUtils.filter_opt_ins(
        individuals, CONSENT_WITHDRAWN_KEY, PipelineConfiguration.RQA_CODING_PLANS))
    for rp in repeat_participations.values():
        rp["% of Participants with Opt-Ins"] = \
            round(rp["Number of Participants with Opt-Ins"] / total_participants * 100, 1)

    # Export the participation frequency data to a csv
    with open(f"{automated_analysis_output_dir}/repeat_participations.csv", "w") as f:
        headers = ["Number of Episodes Participated In", "Number of Participants with Opt-Ins",
                   "% of Participants with Opt-Ins"]
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()

        for row in repeat_participations.values():
            writer.writerow(row)

    log.info(f'Computing repeat and new participation per show ...')
    # Computes the number of new and repeat consented individuals who participated in each radio show.
    # Repeat participants are consented individuals who participated in previous shows prior to the target show.
    # New participants are consented individuals who participated in target show but din't participate in previous shows.
    repeat_new_participation_map = OrderedDict()  # of rqa_raw_field to participation metrics.

    rqa_raw_fields =  [plan.raw_field for plan in PipelineConfiguration.RQA_CODING_PLANS]

    #TODO: update to use responded() once moved to core
    for rqa_raw_field in rqa_raw_fields:
        target_radio_show = rqa_raw_field  # radio show in which we are calculating repeat and new participation metrics for.

        target_radio_show_participants = set()  # contains uids of individuals who participated in target radio show.
        for ind in individuals:
            if ind["consent_withdrawn"] == Codes.TRUE:
                continue

            if target_radio_show in ind:
                target_radio_show_participants.add(ind['uid'])

        previous_radio_shows = []  # rqa_raw_fields of shows that aired before the target radio show.
        for rqa_raw_field in rqa_raw_fields:
            if rqa_raw_field == target_radio_show:
                break

            previous_radio_shows.append(rqa_raw_field)

        previous_radio_shows_participants = set()  # uids of individuals who participated in previous radio shows.
        for rqa_raw_field in previous_radio_shows:
            for ind in individuals:
                if ind["consent_withdrawn"] == Codes.TRUE:
                    continue

                if rqa_raw_field in ind:
                    previous_radio_shows_participants.add(ind['uid'])

        # Check for uids of individuals who participated in target and previous shows.
        repeat_participants = target_radio_show_participants.intersection(previous_radio_shows_participants)

        # Check for uids of individuals who participated in target show but din't participate in previous shows.
        new_participants = target_radio_show_participants.difference(previous_radio_shows_participants)

        repeat_new_participation_map[target_radio_show] = {
            "Radio Show": target_radio_show,  # Todo switch to dataset name
            "No. of opt-in participants": len(target_radio_show_participants),
            "No. of opt-in participants that are new": len(new_participants),
            "No. of opt-in participants that are repeats": len(repeat_participants),
            "% of opt-in participants that are new": None,
            "% of opt-in participants that are repeats": None
        }

        # Compute:
        #  -% of opt-in participants that are new, by computing No. of opt-in participants that are new / No. of opt-in participants
        #  * 100, to 1 decimal place.
        #  - % of opt-in participants that are repeats, by computing No. of opt-in participants that are repeats / No. of opt-in participants
        #  * 100, to 1 decimal place.
        if len(new_participants) > 0:
            repeat_new_participation_map[target_radio_show]["% of opt-in participants that are new"] = \
                round(len(new_participants) / len(target_radio_show_participants) * 100, 1)
            repeat_new_participation_map[target_radio_show]["% of opt-in participants that are repeats"] = \
                round(len(repeat_participants) / len(target_radio_show_participants) * 100, 1)

    with open(f"{automated_analysis_output_dir}/per_show_repeat_and_new_participation.csv", "w") as f:
        headers = ["Radio Show", "No. of opt-in participants", "No. of opt-in participants that are new",
                   "No. of opt-in participants that are repeats", "% of opt-in participants that are new",
                   "% of opt-in participants that are repeats"]
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()

        for row in repeat_new_participation_map.values():
            writer.writerow(row)

    log.info("Computing the demographic distributions...")
    # Count the number of individuals with each demographic code.
    # This count excludes individuals who withdrew consent. STOP codes in each scheme are not exported, as it would look
    # like 0 individuals opted out otherwise, which could be confusing.
    demographic_distributions = OrderedDict()  # of analysis_file_key -> code id -> number of individuals
    total_relevant = OrderedDict()  # of analysis_file_key -> number of relevant individuals
    for plan in PipelineConfiguration.DEMOG_CODING_PLANS:
        for cc in plan.coding_configurations:
            if cc.analysis_file_key is None:
                continue

            demographic_distributions[cc.analysis_file_key] = OrderedDict()
            for code in cc.code_scheme.codes:
                if code.control_code == Codes.STOP:
                    continue
                demographic_distributions[cc.analysis_file_key][code.code_id] = 0
            total_relevant[cc.analysis_file_key] = 0

    for ind in individuals:
        if ind["consent_withdrawn"] == Codes.TRUE:
            continue

        for plan in PipelineConfiguration.DEMOG_CODING_PLANS:
            for cc in plan.coding_configurations:
                if cc.analysis_file_key is None or cc.include_in_theme_distribution == Codes.FALSE:
                    continue

                assert cc.coding_mode == CodingModes.SINGLE
                code = cc.code_scheme.get_code_with_code_id(ind[cc.coded_field]["CodeID"])
                demographic_distributions[cc.analysis_file_key][code.code_id] += 1
                if code.code_type == CodeTypes.NORMAL:
                    total_relevant[cc.analysis_file_key] += 1

    with open(f"{automated_analysis_output_dir}/demographic_distributions.csv", "w") as f:
        headers = ["Demographic", "Code", "Participants with Opt-Ins", "Percent"]
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()

        for plan in PipelineConfiguration.DEMOG_CODING_PLANS:
            for cc in plan.coding_configurations:
                if cc.analysis_file_key is None:
                    continue

                for i, code in enumerate(cc.code_scheme.codes):
                    # Don't export a row for STOP codes because these have already been excluded, so would
                    # report 0 here, which could be confusing.
                    if code.control_code == Codes.STOP:
                        continue

                    participants_with_opt_ins = demographic_distributions[cc.analysis_file_key][code.code_id]
                    row = {
                        "Demographic": cc.analysis_file_key if i == 0 else "",
                        "Code": code.string_value,
                        "Participants with Opt-Ins": participants_with_opt_ins,
                    }

                    # Only compute a percentage for relevant codes.
                    if code.code_type == CodeTypes.NORMAL:
                        if total_relevant[cc.analysis_file_key] == 0:
                            row["Percent"] = "-"
                        else:
                            row["Percent"] = round(participants_with_opt_ins / total_relevant[cc.analysis_file_key] * 100, 1)
                    else:
                        row["Percent"] = ""

                    writer.writerow(row)

    # Compute the theme distributions
    log.info("Computing the theme distributions...")

    def make_survey_counts_dict():
        survey_counts = OrderedDict()
        survey_counts["Total Participants"] = 0
        survey_counts["Total Participants %"] = None
        for plan in PipelineConfiguration.SURVEY_CODING_PLANS:
            for cc in plan.coding_configurations:
                if cc.include_in_theme_distribution == Codes.FALSE:
                    continue

                for code in cc.code_scheme.codes:
                    if code.control_code == Codes.STOP:
                        continue  # Ignore STOP codes because we already excluded everyone who opted out.
                    survey_counts[f"{cc.analysis_file_key}:{code.string_value}"] = 0
                    survey_counts[f"{cc.analysis_file_key}:{code.string_value} %"] = None

        return survey_counts

    def update_survey_counts(survey_counts, td):
        for plan in PipelineConfiguration.SURVEY_CODING_PLANS:
            for cc in plan.coding_configurations:
                if cc.include_in_theme_distribution == Codes.FALSE:
                    continue

                if cc.coding_mode == CodingModes.SINGLE:
                    codes = [cc.code_scheme.get_code_with_code_id(td[cc.coded_field]["CodeID"])]
                else:
                    assert cc.coding_mode == CodingModes.MULTIPLE
                    codes = [cc.code_scheme.get_code_with_code_id(label["CodeID"]) for label in td[cc.coded_field]]

                for code in codes:
                    if code.control_code == Codes.STOP:
                        continue
                    survey_counts[f"{cc.analysis_file_key}:{code.string_value}"] += 1

    def set_survey_percentages(survey_counts, total_survey_counts):
        if total_survey_counts["Total Participants"] == 0:
            survey_counts["Total Participants %"] = "-"
        else:
            survey_counts["Total Participants %"] = \
                round(survey_counts["Total Participants"] / total_survey_counts["Total Participants"] * 100, 1)

        for plan in PipelineConfiguration.SURVEY_CODING_PLANS:
            for cc in plan.coding_configurations:
                if cc.include_in_theme_distribution == Codes.FALSE:
                    continue

                for code in cc.code_scheme.codes:
                    if code.control_code == Codes.STOP:
                        continue

                    code_count = survey_counts[f"{cc.analysis_file_key}:{code.string_value}"]
                    code_total = total_survey_counts[f"{cc.analysis_file_key}:{code.string_value}"]

                    if code_total == 0:
                        survey_counts[f"{cc.analysis_file_key}:{code.string_value} %"] = "-"
                    else:
                        survey_counts[f"{cc.analysis_file_key}:{code.string_value} %"] = \
                            round(code_count / code_total * 100, 1)

    episodes = OrderedDict()
    for episode_plan in PipelineConfiguration.RQA_CODING_PLANS:
        # Prepare empty counts of the survey responses for each variable
        themes = OrderedDict()
        episodes[episode_plan.raw_field] = themes
        for cc in episode_plan.coding_configurations:
            # TODO: Add support for CodingModes.SINGLE if we need it e.g. for IMAQAL?
            assert cc.coding_mode == CodingModes.MULTIPLE, "Other CodingModes not (yet) supported"
            themes["Total Relevant Participants"] = make_survey_counts_dict()
            for code in cc.code_scheme.codes:
                if code.control_code == Codes.STOP:
                    continue
                themes[f"{cc.analysis_file_key}_{code.string_value}"] = make_survey_counts_dict()

        # Fill in the counts by iterating over every individual
        for td in individuals:
            if td["consent_withdrawn"] == Codes.TRUE:
                continue

            relevant_participant = False
            for cc in episode_plan.coding_configurations:
                assert cc.coding_mode == CodingModes.MULTIPLE, "Other CodingModes not (yet) supported"
                for label in td[cc.coded_field]:
                    code = cc.code_scheme.get_code_with_code_id(label["CodeID"])
                    if code.control_code == Codes.STOP:
                        continue
                    themes[f"{cc.analysis_file_key}_{code.string_value}"]["Total Participants"] += 1
                    update_survey_counts(themes[f"{cc.analysis_file_key}_{code.string_value}"], td)
                    if code.code_type == CodeTypes.NORMAL:
                        relevant_participant = True

            if relevant_participant:
                themes["Total Relevant Participants"]["Total Participants"] += 1
                update_survey_counts(themes["Total Relevant Participants"], td)

        set_survey_percentages(themes["Total Relevant Participants"], themes["Total Relevant Participants"])

        for cc in episode_plan.coding_configurations:
            assert cc.coding_mode == CodingModes.MULTIPLE, "Other CodingModes not (yet) supported"

            for code in cc.code_scheme.codes:
                if code.code_type != CodeTypes.NORMAL:
                    continue

                theme = themes[f"{cc.analysis_file_key}_{code.string_value}"]
                set_survey_percentages(theme, themes["Total Relevant Participants"])

    with open(f"{automated_analysis_output_dir}/theme_distributions.csv", "w") as f:
        headers = ["Question", "Variable"] + list(make_survey_counts_dict().keys())
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()

        last_row_episode = None
        for episode, themes in episodes.items():
            for theme, survey_counts in themes.items():
                row = {
                    "Question": episode if episode != last_row_episode else "",
                    "Variable": theme,
                }
                row.update(survey_counts)
                writer.writerow(row)
                last_row_episode = episode

    # Export raw messages labelled with Meta impact, gratitude and about conversation programmatically known as impact/success story
    log.info("Exporting success story raw messages for each episode...")
    impact_messages = []  # of dict of code_string_value to avf-uid and raw messages
    success_story_string_values = ["gratitude", "about_conversation", "impact"]
    for plan in PipelineConfiguration.RQA_CODING_PLANS:
        for cc in plan.coding_configurations:
            for msg in messages:
                if not AnalysisUtils.labelled(msg, CONSENT_WITHDRAWN_KEY, plan):
                    continue

                for label in msg[cc.coded_field]:
                    code = cc.code_scheme.get_code_with_code_id(label["CodeID"])

                    if code.string_value in success_story_string_values:
                        impact_messages.append({
                            "Dataset": plan.dataset_name,
                            "UID": msg['uid'],
                            "Code": code.string_value,
                            "Raw Message": msg[plan.raw_field]
                        })

    with open(f"{automated_analysis_output_dir}/impact_messages.csv", "w") as f:
        headers = ["Dataset", "UID", "Code", "Raw Message"]
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()

        for msg in impact_messages:
            writer.writerow(msg)

    log.info("Automated analysis python script complete")
