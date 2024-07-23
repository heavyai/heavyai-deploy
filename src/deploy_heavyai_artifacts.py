"""
This program works similarly to Terraform in that it takes an input file that
describes the intended final state of on or more Heavy.AI instances. If a
described artifact already exists and is already in the desired configuration, 
it is left untouched. If modifications are required, these are made. If the
artifact doesn't exist, it is created.

Eventually, this script should be replaced with a Terraform plugin.

Requires heavydb >= 7.0
"""

import argparse
import os
import re
import sys

from dotenv import load_dotenv
from heavyai import connect
from io import StringIO as IO

#debug
import traceback
from icecream import ic

from deployment.constants import *
from deployment.validate import validate
from deployment.plan import generate_plan, generate_ddl
from deployment.apply import apply_ddl
from deployment.util import get_file_content, obfuscate_secrets


PROGNAME = os.path.basename(__file__)
PROGDIR = os.path.dirname(os.path.abspath(__file__))


def main() -> None:
    parser = argparse.ArgumentParser(description='Apply initial settings or updates to heavydb instance of the HeavyEco environment in the input JSON doc file.')
    parser.add_argument('command', choices=['validate', 'plan', 'apply'])
    parser.add_argument('--file', metavar='<Path or URI to an artifacts JSON>', required=True,
                        help='The JSON file containing the expected finished environment state.')
    parser.add_argument('--env', metavar='<Path or URI to an env file>',
                        help='The file containing environment variables to use in the JSON file. (Optional)')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Print full DDL statements. (Optional)')
    # not in the initial implementation
    # parser.add_argument('--target', metavar='<artifact or artifact grouping>', nargs='*',
    #                     help='Only apply changes to this artifact or artifact grouping.')

    args = parser.parse_args()

    # ic(args)
    if args.env:
        try:
            load_dotenv(stream=IO(get_file_content(args.env)), override=True, verbose=True)
        except Exception as e:
            print(f'{PROGNAME}: {e}')
            sys.exit(1)

    print('Validating ...')
    try:
        conf = validate(args.file)
    except Exception as e:
        print(f'{PROGNAME}: {e}')
        sys.exit(1)

    print('Syntax validation successful.')
    # ic(conf)
    
    if args.command == 'validate':
        sys.exit(0)

    print('Generating plan ...')
    try:
        url = conf['connection_url']
        print(f'  Connecting to server with {re.sub(RE_OBFUSCATE_DB_URL_PW, RE_OBFUSCATE_DB_URL_PW_REPL, url)}')
        con = connect(url)

        plan = generate_plan(conf, con)
    except Exception as e:
        print(e)

        # for debugging
        #print(traceback.format_exc())

        sys.exit(1)

    print('Plan generation successful.')
    #ic(plan)

    print('Generating DDL ...')
    ddls = generate_ddl(plan)
    print('DDL statements generated for the plan:')
    for ddl in ddls:
        ddl = obfuscate_secrets(ddl)
        if not args.verbose and len(ddl) > VERBOSE_WIDTH:
            ddl = ddl[:VERBOSE_WIDTH] + '...'
        
        if ddl.startswith('-- ('):
            print(f'  {COLORS.HEADER}{ddl}{COLORS.END}')
        else:
            print(f'    {ddl}')

    if args.command == 'plan':
        sys.exit(0)
    
    print('Execute DDL statements? [y/N] ', end='')
    accept = input()
    if accept.lower() != 'y':
        print('Aborting.')
        sys.exit(0)

    print('Applying DDL ...')
    try:
        apply_ddl(con, ddls, args.verbose)
    except Exception as e:
        print(e)
        sys.exit(1)
    
    print('DDL statements applied successfully.')


if __name__ == '__main__':
    main()
