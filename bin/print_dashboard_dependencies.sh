#!/bin/bash
#
# Print all the tables used in a dashboard file or in a HEAVY.AI instance.
#
# usage: print_dashboard_dependencies.py [-h] [--dashboard_file DASHBOARD_FILE] 
#                                        [--dashboard_id DASHBOARD_ID] [--url URL] 
#                                        [--host HOST] [--port PORT] [--user USER] 
#                                        [--passwd PASSWD] [--db DB]
#
# options:
#   -h, --help            show this help message and exit
#   --dashboard_file DASHBOARD_FILE
#                         The name of the file to determine dependencies. Overrides use of database connection args.
#   --dashboard_id DASHBOARD_ID
#                         The integer ID of the dashboard. Must be provided if --url or --host is used.
#   --url URL             Overrides the use of the other connection arguments.
#   --host HOST           
#   --port PORT           Default: 6274
#   --user USER           Default: admin
#   --passwd PASSWD       Default: HyperInteractive
#   --db DB               Default: heavyai
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

python $PROJECT_ROOT/src/print_dashboard_dependencies.py $*
