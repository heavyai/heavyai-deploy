import json
import re

from .constants import *
from .util import get_file_content, replace_env_vars, is_valid_name, is_valid_dashed_name, is_valid_email, is_valid_server_name

def validate(uri: str) -> dict:
    conf_text = get_file_content(uri)

    try:
        conf_text = replace_env_vars(conf_text)
        artifacts = json.loads(conf_text)
    except Exception as e:
        raise RuntimeError(f'Unable to parse json data in {uri}: {e}')

    err_msg = ''

    conf: dict = {}

    try:
        conf['connection_url'] = validate_connection_info(artifacts)
    except RuntimeError as e:
        err_msg += f'  Unable to validate connection information:\n' + str(e)

    try:
        if (retval := validate_configs(artifacts)) is not None:
            conf['configs'] = retval
    except RuntimeError as e:         
        err_msg += f'  Unable to validate configs:\n' + str(e)
    
    try:
        conf['databases'] = validate_databases(artifacts)
    except RuntimeError as e:
        err_msg += f'  Unable to validate databases:\n' + str(e)

    # TODO: ML Model support
    #
    # try:
    #     conf['ml_models'] = validate_ml_models(artifacts)
    # except RuntimeError as e:
    #     err_msg += f'  Unable to validate ML models:\n' + str(e)    

    try:
        if (retval := validate_static_tables(artifacts)) is not None:
            conf['static_tables'] = retval
    except RuntimeError as e:
        err_msg += f'  Unable to validate static tables:\n' + str(e)

    try:
        if (retval := validate_foreign_servers(artifacts)) is not None:
            conf['foreign_servers'] = retval
    except RuntimeError as e:
        err_msg += f'  Unable to validate foreign servers:\n' + str(e)
    
    try:
        if (retval := validate_foreign_tables(artifacts)) is not None:
            conf['foreign_tables'] = retval
    except RuntimeError as e:
        err_msg += f'  Unable to validate foreign tables:\n' + str(e)
    
    try:
        if (retval := validate_dashboards(artifacts)) is not None:
            conf['dashboards'] = retval
    except RuntimeError as e:       
        err_msg += f'  Unable to validate dashboards:\n' + str(e)

    try:
        if (retval := validate_roles(artifacts)) is not None:
            conf['roles'] = retval
    except RuntimeError as e:
        err_msg += f'  Unable to validate roles:\n' + str(e)

    try:
        if (retval := validate_users(artifacts)) is not None:
            conf['users'] = retval
    except RuntimeError as e:
        err_msg += f'  Unable to validate users:\n' + str(e)

    try:
        if (retval := validate_policies(artifacts)) is not None:
            conf['policies'] = retval
    except RuntimeError as e:
        err_msg += f'  Unable to validate policies:\n' + str(e)

    if len(err_msg) != 0:
        raise RuntimeError(f'{COLORS.FAIL}Unable to validate artifacts in {uri}:\n{err_msg}{COLORS.END}')
    
    return conf


def validate_connection_info(server: dict) -> dict:
    err_msg = '';

    if "connection_info" in server:
        ci = server['connection_info']

        if not isinstance(ci, dict):
            raise RuntimeError(f'    Expecting a dictionary for "connection_info"\n')

        if 'url' not in ci:
            if 'host' not in ci:
                ci['host'] = DEFAULT_HOST

            if 'port' not in ci:
                ci['port'] = DEFAULT_PORT
            else:
                try:
                    int(ci['port'])
                except:
                    err_msg += f'    Port must be an integer: {ci["port"]}'


            if 'user' not in ci:
                ci['user'] = DEFAULT_USER

            if 'password' not in ci:
                ci['password'] = DEFAULT_PASSWORD

            if 'database' not in ci:
                ci['database'] = DEFAULT_DATABASE

            ci['url'] = f'heavyai://{ci["user"]}:{ci["password"]}@{ci["host"]}:{ci["port"]}/{ci["database"]}'

        elif not re.match(RE_IS_DB_URL, ci['url']):
            err_msg += f'    Unrecognized database connection URL: {ci["url"]}'

    else:    
        ci = {
            'url': DEFAULT_DB_URL
        }
    
    if len(err_msg) != 0:
        raise RuntimeError(err_msg)

    return ci['url']


def validate_configs(server: dict) -> dict:
    if "configs" not in server:
        return None
    # else:
    #     print("Warning: configs not yet implemented")


def validate_databases(server: dict) -> dict:
    if 'databases' not in server:
        raise RuntimeError(f'    Expecting a "databases" list\n')

    dbs = server['databases']

    err_msg = ''
    retval = []

    if not isinstance(dbs, list):
        raise RuntimeError(f'    Expecting a list for "databases"\n')
    
    for db in dbs:
        if db in retval:
            err_msg += f'    Duplicate database name: {db}\n'
            continue

        if not is_valid_name(db):
            err_msg += f'    Invalid database name: {db}\n' + \
                        '       See https://docs.heavy.ai/sql/data-definition-ddl/users-and-databases#nomenclature-constraints\n'
            continue

        retval.append(db)

    if len(err_msg) != 0:
        raise RuntimeError(err_msg)
    
    return retval


def validate_static_tables(server: dict) -> dict:
    if "static_tables" not in server:
        return None

    static_tables = server['static_tables']

    err_msg = ''
    retval = {}

    if not isinstance(static_tables, dict):
        raise RuntimeError(f'    Expecting a dictionary for "static_tables"\n')

    for db in static_tables:
        if db == '_comment':
            continue

        retval[db] = {}

        if not isinstance(static_tables[db], dict):
            err_msg += f'    Expecting a dictionary for database "{db}"\n'
            continue
        
        for table in static_tables[db]:
            if table == '_comment':
                continue

            if table in retval[db]:
                err_msg += f'    Duplicate static table name: "{table}" in database "{db}"\n'
                continue

            if not isinstance(static_tables[db][table], dict):
                err_msg += f'    Expecting a dictionary for static table "{table}", database "{db}"\n'
                continue
            
            if not is_valid_name(table):
                err_msg += f'    Invalid static table name: "{table}" in database "{db}"\n' + \
                            '       See https://docs.heavy.ai/sql/data-definition-ddl/users-and-databases#nomenclature-constraints\n'
                continue

            retval[db][table] = {}

            if 'ddl_cmd' in static_tables[db][table]:
                if isinstance(static_tables[db][table]['ddl_cmd'], str):
                    retval[db][table]['ddl_cmd'] = static_tables[db][table]['ddl_cmd']
                elif isinstance(static_tables[db][table]['ddl_cmd'], list):
                    retval[db][table]['ddl_cmd'] = ' '.join(static_tables[db][table]['ddl_cmd'])
                else:
                    err_msg += f'    Expecting a string or list of strings for "ddl_cmd" for static table "{table}", database "{db}"\n'
                    continue

            elif 'ddl_uri' in static_tables[db][table]:
                retval[db][table]['ddl_uri'] = static_tables[db][table]['ddl_uri']
            else:
                # only allow the lack of DDL spec if the table is to be
                # imported from a dump file
                #
                if 'import' not in static_tables[db][table] or \
                   'is_dump' not in static_tables[db][table]['import'] or \
                   not re.match(RE_IS_TRUE, static_tables[db][table]['import']['is_dump']):
                    err_msg += f'    Unable to locate "ddl_cmd" or "ddl_uri" keys for static table "{table}", database "{db}"\n'
                    continue

            if 'import' in static_tables[db][table]:
                if not isinstance(static_tables[db][table]['import'], dict):
                    err_msg += f'    Expecting a dictionary for the "import" key of static table "{table}", database "{db}"\n'
                    continue

                if 'source_uri' not in static_tables[db][table]['import']:
                    err_msg += f'    Unable to locate "source_uri"key in "import" for static table "{table}", database "{db}"\n'
                    continue

                retval[db][table]['import'] = {
                    'source_uri': static_tables[db][table]['import']['source_uri'],
                    'is_dump': False
                }

                if 'is_dump' in static_tables[db][table]['import']:
                    retval[db][table]['import']['is_dump'] = True if re.match(RE_IS_TRUE, static_tables[db][table]['import']['is_dump']) else False

                if retval[db][table]['import']['is_dump'] and \
                   retval[db][table]['import']['source_uri'].startswith('http'):
                    err_msg += f'    "source_uri" for static table "{table}", database "{db}": HTTP/S URL not supported for "dump_file" source_type (RESTORE TABLE doesn\'t support it.)\n'
                    continue

                # too many import options to validate here, so just copy them
                # over. import failures will not cause the overall deployment to fail.
                #
                if 'with_clause' in static_tables[db][table]['import']:
                    retval[db][table]['import']['with_clause'] = static_tables[db][table]['import']['with_clause']

            if 'if_exists' in static_tables[db][table]:
                if re.match(RE_IF_EXISTS_RESOURCE, static_tables[db][table]['if_exists']):
                    retval[db][table]['if_exists'] = RESOURCE_IF_EXISTS_ACTIONS[str(static_tables[db][table]['if_exists']).upper()]
                else:
                    err_msg += f'    Unknown argument to "if exists": "{static_tables[db][table]["if_exists"]}" for static table "{table}", database "{db}" (must be one of {", ".join(RESOURCE_IF_EXISTS_ACTIONS.__members__.keys()).lower()})\n'
                    continue
            else:
                retval[db][table]['if_exists'] = DEFAULT_STATIC_TABLE_IF_EXISTS
            

    if len(err_msg) != 0:
        raise RuntimeError(err_msg)
    
    return retval if len(retval) != 0 else None


def validate_foreign_servers(server: dict) -> dict:
    if "foreign_servers" not in server:
        return None

    foreign_servers = server['foreign_servers']

    err_msg = ''
    retval = {}

    if not isinstance(foreign_servers, dict):
        raise RuntimeError(f'    Expecting a dictionary for "foreign_servers"\n')
    
    # not sure why, but foreign servers are tied to a database
    #
    for db in foreign_servers:
        if db == '_comment':
            continue

        retval[db] = {}
    
        for fs in foreign_servers[db]:
            if fs == '_comment':
                continue

            if fs in retval[db]:
                err_msg += f'    Duplicate foreign server name: "{fs}" in database "{db}"\n'
                continue
            
            if not is_valid_name(fs):
                err_msg += f'    Invalid foreign server name: "{fs}" in database "{db}"\n' + \
                            '       See https://docs.heavy.ai/sql/data-definition-ddl/users-and-databases#nomenclature-constraints\n'
                continue

            if not is_valid_server_name(fs):
                err_msg += f'    Invalid foreign server name "{fs}" in database "{db}": may not start with "default", "system", or "internal" (case insensitive).\n' + \
                            '       See https://docs.heavy.ai/heavyconnect/command-reference#create-server\n'
                continue

            retval[db][fs] = {}

            if not isinstance(foreign_servers[db][fs], dict):
                err_msg += f'    Expecting a dictionary for foreign server "{fs}" in database "{db}"\n'
                continue

            if 'wrapper' in foreign_servers[db][fs]:
                if not re.match(RE_DATA_WRAPPERS, foreign_servers[db][fs]['wrapper']):
                    err_msg += f'    Unknown data wrapper "{foreign_servers[db][fs]["wrapper"]}" for foreign server "{fs}" in database "{db}". Must be one of {", ".join(FS_DATA_WRAPPERS)}.\n'
                    continue

                retval[db][fs]['wrapper'] = str(foreign_servers[db][fs]['wrapper']).upper()
            
            else:
                err_msg += f'    Unable to locate "wrapper" key for foreign server "{fs}" in database "{db}"\n'
                continue
            
            if retval[db][fs]['wrapper'] == 'odbc':
                if 'data_source_name' in foreign_servers[db][fs] and 'connection_string' in foreign_servers[db][fs]:
                    err_msg += f'    Only one of "data_source_name" or "connection_string" may be specified for ODBC data wrapper of foreign server "{fs}" in database "{db}"\n'
                    continue

                if 'data_source_name' in foreign_servers[db][fs]:
                    retval[db][fs]['data_source_name'] = foreign_servers[db][fs]['data_source_name']
                    
                if 'connection_string' in foreign_servers[db][fs]:
                    retval[db][fs]['connection_string'] = foreign_servers[db][fs]['connection_string']

                if 'data_source_name' not in retval[db][fs] and 'connection_string' not in retval[db][fs]:
                    err_msg += f'    Unable to locate "connection_string" or "connection_string" key for ODBC data wrapper of foreign server "{fs}" in database "{db}"\n'
                    continue

            # file data wrappers
            #
            else:
                if 'storage_type' in foreign_servers[db][fs]:
                    if not re.match(RE_STORAGE_TYPES, foreign_servers[db][fs]['storage_type']):
                        err_msg += f'    Unknown storage type "{foreign_servers[db][fs]["storage_type"]}" for "{retval[db][fs]["wrapper"]}" data wrapper of foreign server "{fs}" in database "{db}". Must be one of {", ".join(FS_STORAGE_TYPES)}.\n'
                        continue

                    retval[db][fs]['storage_type'] = str(foreign_servers[db][fs]['storage_type']).upper()
                else:
                    err_msg += f'    Unable to locate "storage_type" key for "{retval[fs]["wrapper"]}" data wrapper of foreign server "{fs}" in database "{db}"\n'
                    continue
                
                if 'base_path' in foreign_servers[db][fs]:
                    retval[db][fs]['base_path'] = foreign_servers[db][fs]['base_path']
                elif retval[db][fs]['storage_type'] == 'LOCAL_FILE':
                    err_msg += f'    Unable to locate "base_path" key for "LOCAL_FILE" storage type of foreign server "{fs}" in database "{db}"\n'
                    continue

                if retval[db][fs]['storage_type'] == 'AWS_S3':
                    if 's3_bucket' in foreign_servers[db][fs]:
                        retval[db][fs]['s3_bucket'] = foreign_servers[db][fs]['s3_bucket']

                    if 'aws_region' in foreign_servers[db][fs]:
                        retval[db][fs]['aws_region'] = foreign_servers[db][fs]['aws_region']

                    if 's3_bucket' not in retval[db][fs] or 'aws_region' not in retval[db][fs]:
                        err_msg += f'    Unable to locate "s3_bucket" or "aws_region" key for "AWS_S3" storage type of foreign server "{fs}" in database "{db}" (both are required.)\n'
                        continue


                # TODO: s3_endpoint support. not clear which storage_type it
                #       applies to or what other options are required. The DDL is
                #       accepted by the SQL editor either way and the docs are
                #       ambiguous on this. Need to test and validate what works.
                #
                if 's3_endpoint' in foreign_servers[db][fs]:
                    err_msg += f'    "s3_endpoint" not yet supported for foreign server "{fs}" in database "{db}"\n'
                    continue

                # too much to validate here, so just copy over the options
                #
                if 'user_mapping_with_clause' in foreign_servers[db][fs]:
                    retval[db][fs]['user_mapping_with_clause'] = foreign_servers[db][fs]['user_mapping_with_clause']

    if len(err_msg) != 0:
        raise RuntimeError(err_msg)
    
    return retval


def validate_foreign_tables(server: dict) -> dict:
    if "foreign_tables" not in server:
        return None

    foreign_tables = server['foreign_tables']

    err_msg = ''
    retval = {}

    if not isinstance(foreign_tables, dict):
        raise RuntimeError(f'    Expecting a dictionary for "foreign_tables"\n')
    
    for db in foreign_tables:
        if db == '_comment':
            continue

        retval[db] = {}

        if not isinstance(foreign_tables[db], dict):
            err_msg += f'    Expecting a dictionary for database "{db}"\n'
            continue
        
        for table in foreign_tables[db]:
            if table == '_comment':
                continue

            if table in retval[db]:
                err_msg += f'    Duplicate foreign table name: "{table}" in database "{db}"\n'
                continue

            if not isinstance(foreign_tables[db][table], dict):
                err_msg += f'    Expecting a dictionary for foreign table "{table}", database "{db}"\n'
                continue
            
            if not is_valid_name(table):
                err_msg += f'    Invalid foreign table name: "{table}" in database "{db}"\n' + \
                            '       See https://docs.heavy.ai/sql/data-definition-ddl/users-and-databases#nomenclature-constraints\n'
                continue

            retval[db][table] = {}

            if 'ddl_cmd' in foreign_tables[db][table]:
                if isinstance(foreign_tables[db][table]['ddl_cmd'], str):
                    retval[db][table]['ddl_cmd'] = foreign_tables[db][table]['ddl_cmd']
                elif isinstance(foreign_tables[db][table]['ddl_cmd'], list):
                    retval[db][table]['ddl_cmd'] = ' '.join(foreign_tables[db][table]['ddl_cmd'])
                else:
                    err_msg += f'    Expecting a string or list of strings for "ddl_cmd" for foreign table "{table}", database "{db}"\n'
                    continue

            elif 'ddl_uri' in foreign_tables[db][table]:
                retval[db][table]['ddl_uri'] = foreign_tables[db][table]['ddl_uri']
            else:
                err_msg += f'    Unable to locate "ddl_cmd" or "ddl_uri" keys for foreign table "{table}", database "{db}"\n'
                continue

            if 'server' in foreign_tables[db][table]:
                retval[db][table]['server'] = foreign_tables[db][table]['server']
            else:
                err_msg += f'    Unable to locate "server" key for foreign table "{table}", database "{db}"\n'
                continue

            if 'with_clause' in foreign_tables[db][table]:
                retval[db][table]['with_clause'] = foreign_tables[db][table]['with_clause']

            if 'if_exists' in foreign_tables[db][table]:
                if re.match(RE_IF_EXISTS_RESOURCE, foreign_tables[db][table]['if_exists']):
                    retval[db][table]['if_exists'] = RESOURCE_IF_EXISTS_ACTIONS[str(foreign_tables[db][table]['if_exists']).upper()]
                else:
                    err_msg += f'    Unknown argument to "if exists": "{foreign_tables[db][table]["if_exists"]}" for foreign table "{table}", database "{db}" (must be one of {", ".join(RESOURCE_IF_EXISTS_ACTIONS.__members__.keys()).lower()})\n'
                    continue
            else:
                retval[db][table]['if_exists'] = DEFAULT_FOREIGN_TABLE_IF_EXISTS
            

    if len(err_msg) != 0:
        raise RuntimeError(err_msg)
    
    return retval


def validate_dashboards(server: dict) -> dict:
    if "dashboards" not in server:
        return None

    dashboards = server['dashboards']

    err_msg = ''
    retval = {}

    if not isinstance(dashboards, dict):
        raise RuntimeError(f'    Expecting a dictionary for "dashboards"\n')
    
    for db in dashboards:
        if db == '_comment':
            continue

        retval[db] = {}

        if not isinstance(dashboards[db], dict):
            err_msg += f'    Expecting a dictionary for database "{db}"\n'
            continue
        
        for dash in dashboards[db]:
            if dash == '_comment':
                continue

            if dash in retval[db]:
                err_msg += f'    Duplicate dashboard name: "{dash}" in database "{db}"\n'
                continue

            if not isinstance(dashboards[db][dash], dict):
                err_msg += f'    Expecting a dictionary for dashboard "{dash}", database "{db}"\n'
                continue
            
            retval[db][dash] = {}

            if 'dashboard_uri' in dashboards[db][dash]:
                retval[db][dash]['dashboard_uri'] = dashboards[db][dash]['dashboard_uri']
            else:
                err_msg += f'    Unable to locate "dashboard_uri" key for dashboard "{dash}", database "{db}"\n'
                continue

            if 'if_exists' in dashboards[db][dash]:
                if re.match(RE_IF_EXISTS_RESOURCE, dashboards[db][dash]['if_exists']):
                    retval[db][dash]['if_exists'] = RESOURCE_IF_EXISTS_ACTIONS[str(dashboards[db][dash]['if_exists']).upper()]
                else:
                    err_msg += f'    Unknown argument to "if exists": "{dashboards[db][dash]["if_exists"]}" for dashboard "{dash}", database "{db}" (must be one of {", ".join(RESOURCE_IF_EXISTS_ACTIONS.__members__.keys())})\n'
                    continue
            else:
                retval[db][dash]['if_exists'] = DEFAULT_DASHBOARD_IF_EXISTS

    if len(err_msg) != 0:
        raise RuntimeError(err_msg)
    
    return retval


def validate_roles(server: dict):
    if "roles" not in server:
        return None
    
    roles = server['roles']

    err_msg = ''
    retval = {}

    if not isinstance(roles, dict):
        raise RuntimeError(f'    Expecting a dictionary for "roles"\n')
                                    
    for r in roles:
        if r == '_comment':
            continue

        if r in retval:
            err_msg += f'    Duplicate role name: "{r}"\n'
            continue

        if not is_valid_dashed_name(r):
            err_msg += f'    Invalid role name: "{r}"\n' + \
                        '       See https://docs.heavy.ai/sql/data-definition-ddl/users-and-databases#nomenclature-constraints\n'
            continue

        retval[r] = {}
                    
        if not isinstance(roles[r], dict):
            err_msg += f'    Expecting a dictionary for role "{r}"\n'
            continue
                                    
        if 'databases' in roles[r]:
            retval[r]['databases'] = {}

            dbs = roles[r]['databases']

            if not isinstance(dbs, dict):
                err_msg += f'    Expecting a dictionary for "databases" in role "{r}"\n'
                continue
                                    
            for db in dbs:
                if db == '_comment':
                    continue

                #TODO: if db not in list of databases add to err_msg and continue

                if not isinstance(dbs[db], list):
                    err_msg += f'    Expecting a privileges list for role "{r}", database "{db}"\n'
                    continue

                for p in dbs[db]:
                    if not re.match(RE_PRIVS_DATABASE, p):
                        err_msg += f'    Unknown privilege "{p}" for role "{r}" on database "{db}"\n'
                
                retval[r]['databases'][db] = [ str(p).upper() for p in dbs[db] ]

        else:
            err_msg += f'    Unable to locate "databases" key in role "{r}"\n'

        if 'dashboards' in roles[r]:
            retval[r]['dashboards'] = {}

            dash_defs = roles[r]['dashboards']

            if not isinstance(dash_defs, dict):
                err_msg += f'    Expecting a dictionary for "dashboards" in role "{r}"\n'
                continue
                                    
            for db in dash_defs:
                if db == '_comment':
                    continue

                #TODO: if db not in list of databases add to err_msg and continue

                retval[r]['dashboards'] = { db: {} }

                if not isinstance(dash_defs[db], dict):
                    err_msg += f'    Expecting a dictionary of dashboards for role "{r}", database "{db}"\n'
                    continue

                for dash in dash_defs[db]:
                    if dash == '_comment':
                        continue

                    if not isinstance(dash_defs[db][dash], list):
                        err_msg += f'    Expecting a privileges list for role "{r}", database "{db}", dashboard "{dash}"\n'
                        continue

                    for p in dash_defs[db][dash]:
                        if not re.match(RE_PRIVS_DASHBOARD, p):
                            err_msg += f'    Unknown privilege "{p}" for role "{r}" on database "{db}", dashboard "{dash}"\n'

                    retval[r]['dashboards'][db][dash] = [ str(p).upper() for p in dash_defs[db][dash] ]


    if len(err_msg) != 0:
        raise RuntimeError(err_msg)
    
    return retval


def validate_policies(server: dict) -> dict:
    if 'policies' not in server:
        return None
    
    pol_defs = server['policies']
    
    err_msg = ''
    retval = {}

    if not isinstance(pol_defs, dict):
        raise RuntimeError(f'    Expecting a dictionary for "policies"\n')

    for db in pol_defs:

        if db == '_comment':
            continue

        if not isinstance(pol_defs[db], dict):
            err_msg += f'    Expecting a dictionary for database "{db}"\n'
            continue

        retval[db] = {}

        for tab in pol_defs[db]:

            if tab == '_comment':
                continue

            if not isinstance(pol_defs[db][tab], dict):
                err_msg += f'    Expecting a dictionary for table "{tab}", database "{db}"\n'
                continue

            retval[db][tab] = {}

            for col in pol_defs[db][tab]:

                if col == '_comment':
                    continue

                if not isinstance(pol_defs[db][tab][col], dict):
                    err_msg += f'    Expecting a dictionary for column "{tab}.{col}", database "{db}"\n'
                    continue

                retval[db][tab][col] = {}

                for ur in pol_defs[db][tab][col]:

                    if ur == '_comment':
                        continue

                    if ur in retval[db][tab][col]:
                        err_msg += f'    Duplicate user or role policy definition: for user/role "{ur}", column "{tab}.{col}", database "{db}"\n'
                        continue

                    values = pol_defs[db][tab][col][ur]
                    if not isinstance(values, str) and not isinstance(values, list):
                        err_msg += f'    Expecting a CSV string or list for user or role "{ur}" policy filter value(s), column "{tab}.{col}", database "{db}"\n'
                        continue

                    retval[db][tab][col][ur] = values
    
    return retval


def validate_users(server: dict):
    if "users" not in server:
        return None

    users = server['users']

    err_msg = ''
    retval = {}

    if not isinstance(users, dict):
        raise RuntimeError(f'    Expecting a dictionary for "users"\n')

    for u in users:
        if u == '_comment':
            continue

        if u in retval:
            err_msg += f'    Duplicate user name: "{u}"\n'
            continue

        if not is_valid_dashed_name(u) and not is_valid_email(u):
            err_msg += f'    Invalid user name: "{u}"\n' + \
                        '       See https://docs.heavy.ai/sql/data-definition-ddl/users-and-databases#nomenclature-constraints\n'
            continue

        retval[u] = {}

        if 'password' in users[u]:
            retval[u]['password'] = users[u]['password']
        else:
            retval[u]['password'] = DEFAULT_USER_INIT_PASSWORD
        
        if 'can_login' in users[u]:
            retval[u]['can_login'] = 'true' if re.match(RE_IS_TRUE, users[u]['can_login']) else 'false'
        else:
            retval[u]['can_login'] = DEFAULT_USER_CAN_LOGIN
        
        if 'is_super' in users[u]:
            retval[u]['is_super'] = 'true' if re.match(RE_IS_TRUE, users[u]['is_super']) else 'false'
        else:
            retval[u]['is_super'] = DEFAULT_USER_IS_SUPER
        
        if 'default_db' in users[u]:
            retval[u]['default_db'] = users[u]['default_db']
        else:
            retval[u]['default_db'] = DEFAULT_USER_DATABASE


        if 'roles' in users[u]:
            if not isinstance(users[u]['roles'], list):
                err_msg += f'    Expecting the "roles" key to be a list for user "{u}"\n'
                continue
            
            retval[u]['roles'] = users[u]['roles']

    if len(err_msg) != 0:
        raise RuntimeError(err_msg)

    return retval
