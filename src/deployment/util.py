import base64
import boto3
import hashlib
import json
import os
import re
import requests

from botocore import UNSIGNED
from botocore.client import Config
from botocore.exceptions import NoCredentialsError
from heavyai import Connection
from icecream import ic
from string import whitespace as space
from urllib.parse import urlparse

from .constants import *

AWS_REGION="us-east-1"


def is_valid_name(name: str) -> bool:
    """Check if the name is a valid database object name"""

    return re.match(RE_IS_VALID_NAME, name) is not None


def is_valid_dashed_name(name: str) -> bool:
    """Check if the name is a valid dashed database object name, i.e. a role or user name."""

    return re.match(RE_IS_VALID_DASHED_NAME, name) is not None


def is_valid_email(name: str) -> bool:
    """Check if the name is a valid email address"""

    return re.match(RE_IS_VALID_EMAIL, name) is not None


def is_valid_server_name(name: str) -> bool:
    """Check if the name is a valid HeavyConnect server name"""

    return re.match(RE_INVALID_SERVER_NAMES, name) is None


def parse_ddl_args(text: str) -> list[str]:
    """Parse a string into a list of arguments. Quoted arguments are preserved as a single argument."""

    words = []
    word = []
    state = ARG_PARSE_STATES.SPACE
    quote = ''
    allow_blank = False
    TOKEN_ESCAPE = '\\'

    text_ = iter(text)
    for char in text_:
        if state is ARG_PARSE_STATES.SPACE:
            if char in space:
                continue
            state = ARG_PARSE_STATES.WORD

        if state is ARG_PARSE_STATES.WORD:
            if char in space:
                state = ARG_PARSE_STATES.SPACE
                if allow_blank or word:
                    words.append(''.join(word))
                word = []
                allow_blank = False
                continue
            if char in '\'"':
                quote = char
                allow_blank = True
                state = ARG_PARSE_STATES.QUOTE
        elif state is ARG_PARSE_STATES.QUOTE:
            if char == quote:
                state = ARG_PARSE_STATES.WORD

        if char == TOKEN_ESCAPE and ((state is ARG_PARSE_STATES.WORD) or
                                     (state is ARG_PARSE_STATES.QUOTE and quote != '\'')):
            new_char = next(text_, StopIteration)
            if new_char is StopIteration:
                break
            word.append(char)
            char = new_char
        word.append(char)

    if (state is ARG_PARSE_STATES.WORD or state is ARG_PARSE_STATES.QUOTE):
        if allow_blank or word:
            words.append(''.join(word))

    return words


def replace_env_vars(instr: str) -> str:
    """Scan through a string and replace the usage of any environment variables with their values."""

    retval = instr
    while True:
        m = re.search("(\\$(?:{)?([\\w]+)(?:})?)", retval)
        if not m:
            break

        searchstr = m.group(1)
        varname = m.group(2)

        replstr = os.environ[varname] if varname in os.environ else None
        if not replstr:
            raise RuntimeError(f"Env variable {searchstr} not found.")

        retval = re.sub(f'\\{searchstr}', replstr, retval)

    return retval

def obfuscate_secrets(instr: str) -> str:
    """Scan through a string and replace the usage of any secrets with obfuscated strings."""

    retval = instr

    while True:
        m = re.search(RE_OBFUSCATE_DB_URL_PW, retval)
        if not m:
            break

        searchstr = m.group(1)
        retval = retval.replace(searchstr, RE_OBFUSCATE_DB_URL_PW_REPL)

    while True:
        m = re.search(RE_OBFUSCATE_ODBC_PW, retval)
        if not m:
            break

        searchstr = m.group(1)
        retval = retval.replace(searchstr, RE_OBFUSCATE_ODBC_PW_REPL)

    while True:
        m = re.search(RE_OBFUSCATE_S3_KEY, retval)
        if not m:
            break

        searchstr = m.group(1)

        # s3 secret keys are 40 characters long so we can include the first and
        # last 4 characters in the obfuscated string
        #
        repl_str = searchstr[:4] + '...' + searchstr[-4:]

        retval = retval.replace(searchstr, repl_str)

    return retval


def get_anon_s3_client():
    """Returns an anonymous S3 client.

    Returns
    -------
    An anonymous S3 client
    """
    return boto3.client("s3", config=Config(signature_version=UNSIGNED))


def get_s3_client():
    """Returns an S3 client.

    If the user has a credentials file in its $HOME/.aws folder, the "default" profile will
    be utilized for credentials for accessing S3. If the AWS_PROFILE env variable is 
    defined, this profile will be used instead. The env vars AWS_ACCESS_KEY_ID and
    AWS_SECRET_ACCESS_KEY can also be used.

    If no credentials are available, an anonymous session will be created. The bucket
    must then allow public access or otherwise have permissions allowing the service or
    host running this python script to access it.

    Returns
    -------
    An S3 client
    """

    s3_client = boto3.Session().client('s3')
    try:
        # test the client object on a publicly available file from USGS' S3 bucket
        get_file_content_from_url('s3://prd-tnm/web/css/common.css', s3_client)
    except NoCredentialsError as e:
        s3_client = get_anon_s3_client()

    return s3_client


def resource_exists(url: str, s3_client = None, chase_redirect: bool = False) -> bool:
    """Indicates whether the resource at the given URL exists (True/False)

    Supports both HTTP/S and S3 URI's. 

    Parameters
    ----------
    url : str 
        The location of the resource to look for
    
    s3_client : (An AWS client object created by boto3)
        The client object to use for access. Defaults to None indicating env
        vars, .aws files, or (absent these) anonymous creds should be used to 
        create a client
    
    chase_redirect : bool
        Indicates that if a 302 code is returned by the GET request, the
        redirect code and URL should be returned in a tuple
    
    Returns
    -------
    True if the resource exists, otherwise False (or a redirect tuple if 
    chase_redirect is set.)
    """

    if url.startswith(("http:", "https:")):
        r = requests.head(url)
        if r.status_code == 302 and chase_redirect:
            url_parsed = urlparse(url)
            new_url = f"{url_parsed[0]}://{url_parsed[1]}{r.headers['Location']}"
            r = requests.head(new_url)
            return r.status_code, new_url
        else:
            return r.status_code == 200

    elif url.startswith("s3:"):
        blist = re.findall('^s3://([a-z0-9.-]{3,63})/', url)
        klist = re.findall('^s3://[a-z0-9.-]{3,63}/(.*)', url)

        if not blist or not klist:
            raise RuntimeError(f"Unable to parse S3 bucket URI: {url}")
        
        s3 = s3_client if s3_client else get_s3_client()
        resp = s3.list_objects(Bucket=blist[0], Prefix=klist[0])
        return True if resp.get("Contents") else False

    else:
        raise RuntimeError(f"URL protocol not supported: {url}")


def file_exists(uri: str) -> bool:
    """Check if a file exists on the filesystem or at the HTTP/s or S3 URI."""

    if uri.startswith('http://') or uri.startswith('https://') or uri.startswith('s3://'):
        return resource_exists(uri)
    else:
        return os.path.isfile(uri)


def get_file_content_from_url(url: str, s3_client = None) -> str:
    """Get the contents of the resource at the URL

    Supports both HTTP/S and S3 URI's. 

    Parameters
    ----------
    url : str 
        The location of the resource to download
    
    s3_client : (An AWS client object created by boto3)
        The client object to use for access. Defaults to None indicating env
        vars, .aws files, or (absent these) anonymous creds should be used to 
        create a client
    
    Returns
    -------
    The contents of the resource as a string
    """

    if url.startswith(("http:", "https:")):
        return requests.get(url).text

    elif url.startswith("s3:"):
        blist = re.findall('^s3://([a-z0-9.-]{3,63})/', url)
        klist = re.findall('^s3://[a-z0-9.-]{3,63}/(.*)', url)

        if not blist or not klist:
            raise RuntimeError(f"Unable to parse S3 bucket URI: {url}")
        
        s3 = s3_client if s3_client else get_s3_client()
        obj = s3.get_object(Bucket=blist[0], Key=klist[0])
        contents = obj['Body'].read()
        return contents.decode("utf-8")

    else:
        raise RuntimeError(f"URL protocol not supported: {url}")


def get_file_content(uri: str) -> str:
    """Get the contents of a file from the filesystem or at the HTTP/s or S3 URI."""

    if uri.startswith('http://') or uri.startswith('https://') or uri.startswith('s3://'):
        return get_file_content_from_url(uri)
    else:
        if not os.path.isfile(uri):
            raise RuntimeError(f'File {uri} not found.')

        with open(uri, 'r') as f:
            return f.read()


def is_dash_code_same(con: Connection, db_name: str, dash_id: int, dash_file: str) -> bool:
    """
    Performs a hash comparison of the view states of a dashboard in a database 
    and a dashboard export file.
    """

    # capture the current database from the session info
    #
    orig_db = con._client.get_session_info(con._session).database
    con._client.switch_database(con._session, db_name)

    dashboard = con._client.get_dashboard(con._session, dash_id)

    # construct what the dashboard export file contents would look like
    #
    db_view_state = base64.b64decode(dashboard.dashboard_state).decode('utf-8').rstrip()

    # switch back to the database in use prior to the call
    # to is_dash_code_same()
    #
    con._client.switch_database(con._session, orig_db)

    file_view_state = get_file_content(dash_file).split('\n')[2].rstrip()

    # get the md5 hashes for the existing dashboard and the new dashboard
    #
    existing_md5 = hashlib.md5(db_view_state.encode('utf-8')).hexdigest()
    new_md5 = hashlib.md5(file_view_state.encode('utf-8')).hexdigest()

    return existing_md5 == new_md5

def get_dash_id_from_name(con: Connection, db_name: str, dash_name: str) -> int:
    """Looks up the name of a dashboard from its ID in a database."""

    dbs = [ d.db_name for d in con._client.get_databases(con._session) ]
    if db_name not in dbs:
        return -1

    # capture the current database from the session info and switch to the
    # database containing the dashboard.
    #
    orig_db = con._client.get_session_info(con._session).database
    con._client.switch_database(con._session, db_name)

    # get the dashboard id from the dashboard name. this works, whereas 
    # selecting from the information_schema.dashboards table does not.
    # there seems to be either a race condition or some kind of commit 
    # visibility issue after creating a dashboard.
    #
    dash_list = con._client.get_dashboards(con._session)
    dash_ids = [ d.dashboard_id for d in dash_list if d.dashboard_name == dash_name ]
    dash_id = dash_ids[0] if len(dash_ids) > 0 else -1

    # switch back to the database in use prior to the call to this method.
    #
    con._client.switch_database(con._session, orig_db)

    return dash_id


def exec_dash_ddl(con: Connection, ddl: str) -> None:
    r"""
    There are no DDL or slash commands for working with dashboards (other than
    '\import_dashboard') so this implements support for them. Supported
    commands are:

    \drop_dashboard "dashboard_name"
    
    \rename_dashboard "dashboard_name" "new_dashboard_name"
    
    \import_dashboard "dashboard_name" "file_uri"

    NOTE: Arguments must be double quoted, not single quoted.
    """
    ddl = ddl.strip()
    db = con._client.get_session_info(con._session).database

    # parse single quoted and escaped double quotes arguments in ddl
    #
    args = parse_ddl_args(ddl)

    cmd = args[0]

    if len(args) >= 2:
        dash = args[1][1:-1] if args[1].startswith('"') else args[1]
        dash = dash.replace('\\"', '"')
    else:
        raise RuntimeError(f'Missing arguments to {cmd}: {ddl}')
    
    if cmd != '\\drop_dashboard':
        if len(args) < 3:
            raise RuntimeError(f'Missing arguments to {cmd}: {ddl}')
    
    dash_id = get_dash_id_from_name(con, db, dash) if cmd != '\\import_dashboard' else None

    match cmd:
        case '\\drop_dashboard':
            con._client.delete_dashboard(con._session, dash_id)

        case '\\rename_dashboard':
            new_dash = args[2][1:-1] if args[2].startswith('"') else args[2]
            new_dash = new_dash.replace('\\"', '"')

            # there doesn't seem to be a way in the heavyai API to rename a
            # dashboard, so we have to duplicate it and delete the original.
            #
            con.duplicate_dashboard(dash_id, new_dash)
            con._client.delete_dashboard(con._session, dash_id)

        case '\\import_dashboard':
            dash_file_uri = args[2][1:-1] if args[2].startswith('"') else args[2]
            dash_file_uri = dash_file_uri.replace('\\"', '"')
            dname, dmeta, dstate = get_file_content(dash_file_uri).split('\n', 2)

            con._client.create_dashboard(
                con._session, 
                dash, 
                base64.b64encode(bytes(dstate.encode('utf-8'))), 
                None, 
                dmeta
            )

        case _:
            raise RuntimeError(f'Unrecognized dashboard DDL: {ddl}')


def get_dash_table_deps_from_db(con: Connection, db_name: str, dash_id: int) -> list[str]:
    """Get the list of tables used in a dashboard from a database connection."""

    orig_db = con._client.get_session_info(con._session).database
    con._client.switch_database(con._session, db_name)

    dash_obj = con._client.get_dashboard(con._session, dash_id)
 
    con._client.switch_database(con._session, orig_db)

    dash_dict = json.loads(base64.b64decode(dash_obj.dashboard_state).decode('utf-8'))

    return get_dash_table_deps(dash_dict)


def get_dash_table_deps(dash: dict) -> list[str]:
    """
    Get the list of tables used in a dashboard from the dashboard export file's
    dashboard state (line 3) translated into a dictionary.
    """

    # extract all the data sources from all the tabs. dedup the list
    # using a dictionary.
    #
    data_sources = {}
    for tab in dash['tabs']:
        for d in dash['tabs'][tab]['dashboard']['dataSources'].keys():
            data_sources[d] = 1

    # look for any Immerse Joins, i.e. ${-N...}, in the data sources list and
    # look up the tables involved in the join in the dashboard's parameters
    # list. dedup again using a dictionary.
    #
    # NOTE: there isn't an easy and complete way of dealing with custom
    # sources as we would need an SQL parser that can handle heavydb's
    # syntactic extensions.
    #
    tabs = {}
    for t in data_sources.keys():
        if t.startswith(r'${'):
            p = re.match(r'\${(.*)}', t).group(1)
            v = dash['parameters']['definitions'][p]['defaultValue']

            if dash['parameters']['definitions'][p]['type'] == 'JOIN':
                m = re.match(r'"([^"]?)" (?:[A-Z]? )*JOIN "([^"]?)"', v)
                if m is not None:
                    tabs[m.group(1)] = 1
                    tabs[m.group(2)] = 1
            
            else: # 'TABLE'
                # too complex to parse the SQL, so just punt. need a proper
                # SQL parser to do this. would need to be able to deal with
                # TABLE function calls as well as CURSOR arguments to same.
                #
                pass

        else:
            tabs[t] = 1


    return sorted(tabs.keys())
