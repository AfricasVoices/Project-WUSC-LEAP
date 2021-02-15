#!/usr/bin/env bash

set -e

if [[ $# -ne 3 ]]; then
    echo "Usage: ./1_coda_get.sh <coda-auth-file> <coda-v2-root> <data-root>"
    echo "Downloads coded messages datasets from Coda to '<data-root>/Coded Coda Files'"
    exit
fi

AUTH=$1
CODA_V2_ROOT=$2
DATA_ROOT=$3

./checkout_coda_v2.sh "$CODA_V2_ROOT"

DATASETS=(
    "WUSC-LEAP_kalobeyei_s01e01"
    "WUSC-LEAP_kalobeyei_s01e02"
    "WUSC-LEAP_kalobeyei_s01e03"
    "WUSC-LEAP_kalobeyei_s01e04"
    "WUSC-LEAP_kalobeyei_s01e05"

    "WUSC-KEEP-II_kakuma_location"
    "WUSC-KEEP-II_kakuma_gender"
    "WUSC-KEEP-II_kakuma_age"
    "WUSC-KEEP-II_kakuma_nationality"
    "WUSC-KEEP-II_kakuma_household_language"

    "WUSC-LEAP_kalobeyei_participants_engaging"
    "WUSC-LEAP_kalobeyei_targeted_group_parent"
    "WUSC-LEAP_kalobeyei_child_gender"
    "WUSC-LEAP_kalobeyei_consent_to_engage_child"
    "WUSC-LEAP_kalobeyei_age_of_parent"
    "WUSC-LEAP_kalobeyei_currently_in_school"
)

cd "$CODA_V2_ROOT/data_tools"
git checkout "c47977d03f96ba3e97c704c967c755f0f8b666cb"  # (master which supports incremental get)

mkdir -p "$DATA_ROOT/Coded Coda Files"

for DATASET in ${DATASETS[@]}
do
    FILE="$DATA_ROOT/Coded Coda Files/$DATASET.json"

    if [ -e "$FILE" ]; then
        echo "Getting messages data from ${DATASET} (incremental update)..."
        MESSAGES=$(pipenv run python get.py --previous-export-file-path "$FILE" "$AUTH" "${DATASET}" messages)
        echo "$MESSAGES" >"$FILE"
    else
        echo "Getting messages data from ${DATASET} (full download)..."
        pipenv run python get.py "$AUTH" "${DATASET}" messages >"$FILE"
    fi

done
