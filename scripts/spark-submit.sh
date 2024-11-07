#!/usr/bin/env bash

set -eu -o pipefail

: "${AUTH_KEY:?}"
: "${AWS_ACCESS_KEY_ID:?}"
: "${AWS_SECRET_ACCESS_KEY:?}"

AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-https://s3.direct.us-south.cloud-object-storage.appdomain.cloud}"

if (( $# != 2 )); then
    echo "Usage: $(basename -- "${BASH_SOURCE[0]}") <analytics-engine-instance-ID> <application-path>" >&2
    exit 1
fi

ANALYTICS_ENGINE_ID="$1"
SPARK_APP_COS_PATH="$2"
SPARK_APP_COS_PATH="cos://${SPARK_APP_COS_PATH/\//.data/}"

SCRIPT_DIR="$( cd "$( dirname -- "${BASH_SOURCE[0]}" )" > /dev/null && pwd )"

oauth_token() {
    curl --request POST --silent \
      'https://iam.cloud.ibm.com/identity/token' \
      --header 'Content-Type: application/x-www-form-urlencoded' \
      --data "grant_type=urn:ibm:params:oauth:grant-type:apikey&apikey=${AUTH_KEY}" \
      | jq --raw-output '.access_token'
}

submit_json() {
    local JQ_EXPRESSION
    # shellcheck disable=SC2016
    JQ_EXPRESSION='.application_details.application |= $APP_URL
 | .application_details.env.AUTH_KEY |= $AUTH_KEY
 | .application_details.conf."spark.hadoop.fs.cos.data.access.key" |= $ACCESS_KEY
 | .application_details.conf."spark.hadoop.fs.cos.data.secret.key" |= $SECRET_KEY
 | .application_details.conf."spark.hadoop.fs.cos.data.endpoint" |= $ENDPOINT'
    jq --compact-output \
        --arg APP_URL "${SPARK_APP_COS_PATH}" \
        --arg AUTH_KEY "${AUTH_KEY}" \
        --arg ACCESS_KEY "${AWS_ACCESS_KEY_ID}" \
        --arg SECRET_KEY "${AWS_SECRET_ACCESS_KEY}" \
        --arg ENDPOINT "${AWS_ENDPOINT_URL}" \
        "${JQ_EXPRESSION}" \
        "${SCRIPT_DIR}/spark-submit.json"
}

submit_application() {
    curl --request POST --silent \
    "https://api.eu-de.ae.cloud.ibm.com/v3/analytics_engines/${ANALYTICS_ENGINE_ID}/spark_applications" \
    --header "Authorization: Bearer ${IAM_TOKEN}" \
    --header 'Content-Type: application/json' \
    --data "$1"
}

get_application_info() {
    curl --request GET --silent \
        "https://api.eu-de.ae.cloud.ibm.com/v3/analytics_engines/${ANALYTICS_ENGINE_ID}/spark_applications/$1" \
        --header "Authorization: Bearer ${IAM_TOKEN}"
}

parse_id() {
    jq --raw-output '.id' <<< "$1"
}

parse_state() {
    jq --raw-output '.state' <<< "$1"
}

wait_for_completion() {
    local APPLICATION_ID APPLICATION_STATE RESPONSE

    RESPONSE="$1"
    APPLICATION_ID="$( parse_id "${RESPONSE}" )"
    APPLICATION_STATE="$( parse_state "${RESPONSE}" )"

    echo "Application ${APPLICATION_ID} ${APPLICATION_STATE}"

    while [[ ${APPLICATION_STATE} == accepted ]] || [[ ${APPLICATION_STATE} == running ]]; do
        sleep 2
        RESPONSE="$( get_application_info "${APPLICATION_ID}" )"
        APPLICATION_STATE="$( parse_state "${RESPONSE}" )"
        echo -n '.'
    done

    echo ''
    if [[ ${APPLICATION_STATE} == finished ]]; then
        echo 'Finished'
    else
        echo "${RESPONSE}" | jq >&2
        exit 2
    fi
}

IAM_TOKEN="$( oauth_token )"
SUBMIT_JSON="$( submit_json )"
RESPONSE="$( submit_application "${SUBMIT_JSON}" )"
wait_for_completion "${RESPONSE}"
