import argparse
import json
import os

from heavyai import connect
from deployment.util import get_dash_table_deps, get_dash_table_deps_from_db

def get_connect_url(args):
    if args.url is not None:
        url = args.url
    elif args.host is not None:
        url = f"heavyai://{args.user}:{args.passwd}@{args.host}:{args.port}/{args.db}"
    elif 'HEAVYAI_DB_URL' in os.environ:
        url = os.getenv('HEAVYAI_DB_URL')
    else:
        url = f"heavyai://{args.user}:{args.passwd}@localhost:{args.port}/{args.db}"

    return url

def print_dependencies(args) -> None:
    dash_table_deps: list = None

    if args.dashboard_file is not None:
        try:
            with open(args.dashboard_file, 'r') as f:
                dashboard_json = f.read().split('\n')[2]
        except Exception as e:
            print(f'Error: Could not open file {args.dashboard_file}. {e}')
            return
        
        try:
            dash_dict = json.loads(dashboard_json)
        except Exception as e:
            print(f'Error: Could not parse JSON from file {args.dashboard_file}. {e}')
            return
        
        dash_table_deps = get_dash_table_deps(dash_dict)
    
    else:
        url = get_connect_url(args)
        con = connect(url)
        dash_table_deps = get_dash_table_deps_from_db(con, args.db, args.dashboard_id)

    for t in dash_table_deps:
        print(t)

def main():
    my_parser = argparse.ArgumentParser(description='Print all the tables used in a dashboard file or in a HEAVY.AI instance.')

    # add the supported arguments
    #
    my_parser.add_argument('--dashboard_file', action='store', type=str,
                           help='The name of the file to determine dependencies. Overrides use of database connection args.')
    my_parser.add_argument('--dashboard_id', action='store', type=int,
                           help='The integer ID of the dashboard. Must be provided if --url or --host is used.')
    my_parser.add_argument('--url', action='store', type=str,
                           help='The URL of the HeavyAI instance. Overrides the use of the other connection arguments.')
    my_parser.add_argument('--host', action='store', type=str,
                           help='The hostname of the HeavyAI instance.')
    my_parser.add_argument('--port', action='store', type=int, default=6274,
                           help='The port number of the HeavyAI instance. Default: 6274.')
    my_parser.add_argument('--user', action='store', type=str, default='admin',
                           help='The username to use to connect to the HeavyAI instance. Default: admin.')
    my_parser.add_argument('--passwd', action='store', type=str, default='HyperInteractive',
                           help='The password to use to connect to the HeavyAI instance. Default: HyperInteractive.')
    my_parser.add_argument('--db', action='store', type=str, default='heavyai',
                           help='The database name to use to connect to the HeavyAI instance. Default: heavyai.')

    args = my_parser.parse_args()
    print_dependencies(args)

if __name__ == '__main__':
    main()
