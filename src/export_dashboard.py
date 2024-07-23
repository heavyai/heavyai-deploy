import argparse
import base64
import os

from heavyai import connect

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

def export_dashboard(args) -> None:
    url = get_connect_url(args)
    con = connect(url)
    dashboard = con._client.get_dashboard(con._session, args.dashboard_id)
    view_state = base64.b64decode(dashboard.dashboard_state).decode('utf-8')
    dashbordexport = f"{dashboard.dashboard_name}\n{dashboard.dashboard_metadata}\n{view_state}"

    print(f'Exporting dashboard "{dashboard.dashboard_name}" (id = {args.dashboard_id})')

    with open(args.outfile, 'w') as f:
        f.write(dashbordexport)

 
def main():
    my_parser = argparse.ArgumentParser(description='Export a dashboard from a HeavyAI instance to a file.')

    # add the supported arguments
    #
    my_parser.add_argument('--dashboard_id', action='store', type=int, required=True,
                           help='The integer ID of the dashboard.')
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
    my_parser.add_argument('--outfile', action='store', type=str, default='heavyai_dashboard.json',
                           help='The name of the file to export the dashboard to. Default: heavyai_dashboard.json.')

    args = my_parser.parse_args()
    export_dashboard(args)
    
    print(f'Dashboard exported successfully to {args.outfile}.')

if __name__ == '__main__':
    main()
