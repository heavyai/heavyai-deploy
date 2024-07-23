#!/bin/bash
#
# Export a HEAVY.AI dashboard to a file.
#
# usage: export_dashboard.py [-h] --dashboard_id DASHBOARD_ID [--url URL]
#                            [--host HOST] [--port PORT] [--user USER] 
#                            [--passwd PASSWD] [--db DB] [--outfile OUTFILE]
#
# options:
#   -h, --help                  show this help message and exit
#   --dashboard_id DASHBOARD_ID
#   --url URL                   URL of the HEAVY.AI server (default: heavyai://admin:HyperInteractive@localhost:6274/heavyai)
#   --host HOST                 (default: localhost)
#   --port PORT                 (default: 6274)
#   --user USER                 (default: admin)
#   --passwd PASSWD             (default: HyperInteractive)
#   --db DB                     (default: heavyai)
#   --outfile OUTFILE           (default: heavyai_dashboard.json)
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

python $PROJECT_ROOT/src/export_dashboard.py $*
