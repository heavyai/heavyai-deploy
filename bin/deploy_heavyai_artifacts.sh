#!/bin/bash
#
# Apply initial settings or updates to heavydb instance of the HeavyEco environment 
# in the input JSON doc file.
#
# usage: deploy_heavyai_artifacts.sh [-h] --file <Path or URI to an artifacts JSON> 
#                                    [--env <Path or URI to an env file>] [--verbose]
#                                    {validate, plan, apply}
#
# positional arguments. must specify one and only one:
#   {validate, plan, apply}
#
# options:
#   -h, --help            show this help message and exit
#   --file <Path or URI to an artifacts JSON>
#                         The JSON file containing the expected finished environment state.
#   --env <Path or URI to an env file>
#                         The file containing environment variables to use in the JSON file. (Optional)
#   --verbose, -v         Print full DDL statements. (Optional)
#
#set -x
PROGFILE=`/usr/bin/realpath $0`
PROGDIR=`dirname $PROGFILE`
PROGNAME=`basename $PROGFILE`

PROJECT_ROOT=`dirname $PROGDIR`
. $PROJECT_ROOT/src/project.env

# check that the conda executable is accessible
#
which conda > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo; echo -n "  ERROR: Mambaforge does not appear to be accessible. Please check that "
    echo "it has been installed and is accessible in your PATH environment variable."
    exit -1
fi

# Checking for Python environment ($PY_ENV)
#
eval "$(conda shell.bash hook)"
conda env list | egrep ^$PY_ENV > /dev/null
if [ $? -ne 0 ]; then
    echo "Python environment '$PY_ENV' not found."
    echo "Please create it using: mamba env create -f $PROJECT_ROOT/environment_dev.yml"
    exit -1
fi

conda activate $PY_ENV

python $PROJECT_ROOT/src/deploy_heavyai_artifacts.py $*
