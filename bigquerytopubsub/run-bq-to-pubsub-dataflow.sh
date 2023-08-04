#!/bin/bash

# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

print_usage () { 
    echo "Usage: $(basename $0) [-p <project name>]
    [-n <number of dataflow workers>]
    [-r <region>]
    [-d <dataset>]
    [-t <table>]
    [-o <topic>]
    [-s <subnetwork>]
    [-b <bucket>]" 1>&2 
    exit 1 
}

# If we didn't get any parameters, print usage and exit
[ $# -eq 0 ] && print_usage

# Parse the necessary parameters to run the dataflow job
while getopts "p:n:r:s:b:d:t:o:" arg; do
    case $arg in
        p)
            PROJECT=${OPTARG}
            ;;
        n)
            NUM_WORKERS=${OPTARG}
            ;;
        r)
            REGION=${OPTARG}
            ;;
        s)
            SUBNETWORK=${OPTARG}
            ;;
        b)
            BUCKET=${OPTARG}
            ;;
        d)
            DATASET=${OPTARG}
            ;;
        t)
            TABLE=${OPTARG}
            ;;
        o)
            TOPIC=${OPTARG}
            ;;
    esac
done
shift $((OPTIND-1))

# If any of the necessary env vars are not set, print usage and exit
# Sure hope we don't get here
if [ -z "${PROJECT}" ] || [ -z "${NUM_WORKERS}" ] || [ -z "${REGION}" ] || [ -z "${SUBNETWORK}" ] || [ -z "${BUCKET}" ] || [ -z "${DATASET}" ] || [ -z "${TABLE}" ] || [ -z "${TOPIC}" ]; then
    print_usage
fi

#echo "project set to ${PROJECT}"
#echo "num_workers set to ${NUM_WORKERS}"
#echo "region set to ${REGION}"
#echo "subnetwork set to ${SUBNETWORK}"
#echo "bucket set to ${BUCKET}"

# Exit if we can't find maven executable on the PATH
if ! mvn -v mvn &> /dev/null
then
    echo "'mvn' could not be found. Be sure to include the maven executable in your PATH."
    exit 1
fi

# Execute Dataflow
mvn -X -Pdataflow-runner compile exec:java \
    -Dexec.mainClass=BigQueryToPubSub \
    -Dexec.args="--project=${PROJECT} \
    --dataset=${DATASET} \
    --table=${TABLE} \
    --topic=${TOPIC} \
    --gcpTempLocation=gs://${BUCKET} \
    --runner=DataflowRunner \
    --experiments=use_runner_v2 \
    --numWorkers=${NUM_WORKERS} \
    --autoscalingAlgorithm=NONE \
    --region=${REGION} \
    --subnetwork=https://www.googleapis.com/compute/v1/projects/${PROJECT}/regions/${REGION}/subnetworks/${SUBNETWORK} \
    --usePublicIps=false"
