import csv
import json
import pandas as pd
import random
import string
import warnings

from datetime import datetime
from heavyai import Connection
from icecream import ic

from .constants import *
from .util import file_exists, is_dash_code_same, get_dash_id_from_name, get_file_content, get_dash_table_deps


def generate_plan(conf: dict, con: Connection) -> dict:
    plan: dict = {}

    err_msg = ''

    con._client.switch_database(con._session, 'information_schema')
    dbs = [ d.db_name for d in con._client.get_databases(con._session) ]
    if 'heavyai' not in dbs and 'omnisci' not in dbs and 'mapd' not in dbs:
        raise RuntimeError(f'{COLORS.FAIL}Planning failed:\nUnable to identify default database (one of "heavyai", "omnisci", or "mapd"){COLORS.END}')

    plan['default_database'] = 'heavyai' if 'heavyai' in dbs else \
                               'omnisci' if 'omnisci' in dbs else \
                               'mapd'


    if 'configs' in conf:
        try:
            plan['configs'] = plan_configs(con, conf, plan)
        except Exception as e:
            err_msg += f'  Error planning configs: \n{e}\n'
    
    if 'databases' in conf:
        try:
            plan['databases'] = plan_databases(con, conf, plan)
        except Exception as e:
            err_msg += f'  Error planning databases: \n{e}\n'
    
    if 'static_tables' in conf:
        try:
            plan['static_tables'] = plan_static_tables(con, conf, plan)
        except Exception as e:
            err_msg += f'  Error planning static tables: \n{e}\n'
    
    if 'foreign_servers' in conf:
        try:
            plan['foreign_servers'] = plan_foreign_servers(con, conf, plan)
        except Exception as e:
            err_msg += f'  Error planning foreign servers: \n{e}\n'
    
    if 'foreign_tables' in conf:
        try:
            plan['foreign_tables'] = plan_foreign_tables(con, conf, plan)
        except Exception as e:
            err_msg += f'  Error planning foreign tables: \n{e}\n'
    
    if 'dashboards' in conf:
        try:
            plan['dashboards'] = plan_dashboards(con, conf, plan)
        except Exception as e:
            err_msg += f'  Error planning dashboards: \n{e}\n'

    if 'roles' in conf:
        try:
            plan['roles'] = plan_roles(con, conf, plan)
        except Exception as e:
            err_msg += f'  Error planning roles: \n{e}\n'

    if 'policies' in conf:
        try:
            plan['policies'] = plan_policies(con, conf, plan)
        except Exception as e:
            err_msg += f'  Error planning policies: \n{e}\n'

    if 'users' in conf:
        try:
            plan['users'] = plan_users(con, conf, plan)
        except Exception as e:
            err_msg += f'  Error planning users: \n{e}\n'
    
    if len(err_msg) != 0:
        raise RuntimeError(f'{COLORS.FAIL}Planning failed:\n{err_msg}{COLORS.END}')

    return plan


def plan_configs(con: Connection, server_conf: dict, server_plan: dict) -> dict:
    return None


def plan_databases(con: Connection, server_conf: dict, server_plan: dict) -> dict:
    databases_plan = {}

    dbs = [ d.db_name for d in con._client.get_databases(con._session) ]

    for db in server_conf['databases']:
        databases_plan[db] = {}

        databases_plan[db]['state'] = RESOURCE_STATES.NEEDS_CREATION if db not in dbs else \
                                      RESOURCE_STATES.EXISTS

    return databases_plan


def plan_static_tables(con: Connection, server_conf: dict, server_plan: dict) -> dict:
    static_tables_plan = {}
    err_msg = ''

    dbs = [ d.db_name for d in con._client.get_databases(con._session) ]

    for db in server_conf['static_tables']:
        if db not in dbs and \
           ('databases' not in server_plan or db not in server_plan['databases']):
            err_msg += f'    Unable to create/modify static tables in database "{db}": Database does not exist and not in plan.\n'
            continue

        static_tables_plan[db] = {}

        with warnings.catch_warnings():
            warnings.simplefilter(action='ignore', category=UserWarning)
            tab_list = pd.read_sql_query(f"SELECT table_name FROM tables WHERE database_name = '{db}'", con)['table_name'].values

        for tab in server_conf['static_tables'][db]:
            if tab == '_comment':
                continue

            static_tables_plan[db][tab] = server_conf['static_tables'][db][tab]
            static_tables_plan[db][tab]['state'] = RESOURCE_STATES.EXISTS if tab in tab_list else \
                                                   RESOURCE_STATES.NEEDS_CREATION

            if 'ddl_uri' in static_tables_plan[db][tab] and \
               (static_tables_plan[db][tab]['state'] == RESOURCE_STATES.NEEDS_CREATION or \
                static_tables_plan[db][tab]['if_exists'] != RESOURCE_IF_EXISTS_ACTIONS.SKIP) and \
               not file_exists(static_tables_plan[db][tab]['ddl_uri']):
                err_msg += f'    Unable to create static table "{tab}" in database "{db}" from "ddl_uri": File "{static_tables_plan[db][tab]["ddl_uri"]}" not found.\n'
                continue

            # TODO: import data can come from ODBC and custom s3 endpoints.
            #       need to figure out how to handle those.
            #
            if 'import' in static_tables_plan[db][tab] and \
               (static_tables_plan[db][tab]['state'] == RESOURCE_STATES.NEEDS_CREATION or \
                static_tables_plan[db][tab]['if_exists'] != RESOURCE_IF_EXISTS_ACTIONS.SKIP) and \
               static_tables_plan[db][tab]['import']['is_dump'] and \
               not file_exists(static_tables_plan[db][tab]['import']['source_uri']):
                err_msg += f'    Unable to import data into static table "{tab}" in database "{db}" from "source_uri": File "{static_tables_plan[db][tab]["import"]["source_uri"]}" not found.\n'
                continue

            if not 'import' in static_tables_plan[db][tab] or \
               not static_tables_plan[db][tab]['import']['is_dump']:

                if 'ddl_cmd' in static_tables_plan[db][tab]:
                    ddl_cmd = static_tables_plan[db][tab]['ddl_cmd']
                else:
                    ddl_cmd = get_file_content(static_tables_plan[db][tab]['ddl_uri'])
                
                static_tables_plan[db][tab]['ddl_cmd'] = ddl_cmd
                
                if not re.match(RE_IS_CREATE_STATIC_TABLE_DDL, ddl_cmd):
                    err_msg += f'    Unable to create static table "{tab}" in database "{db}": DDL does not appear to be a CREATE TABLE statement.\n'
                    continue
        

    if len(err_msg) != 0:
        raise RuntimeError(err_msg)

    return static_tables_plan


def plan_foreign_servers(con: Connection, server_conf: dict, server_plan: dict) -> dict:
    foreign_servers_plan = {}
    err_msg = ''

    dbs = [ d.db_name for d in con._client.get_databases(con._session) ]

    for db in server_conf['foreign_servers']:
        if db not in dbs and \
           ('databases' not in server_plan or db not in server_plan['databases']):
            err_msg += f'    Unable to create/modify servers in database "{db}": Database does not exist and not in plan.\n'
            continue

        foreign_servers_plan[db] = {}

        # there's no "servers" table in the "information_schema" database, so
        # we have to switch to the database in question in order to use SHOW
        # SERVERS to get the list of servers. start by creating an empty result
        # set with the correct schema.
        #
        schema={'server_name': 'object', 'data_wrapper': 'object', 'created_at': 'datetime64[ns]', 'options': 'object'}
        servers_list = pd.DataFrame(columns=schema.keys()).astype(schema)
        if server_plan['databases'][db]['state'] == RESOURCE_STATES.EXISTS:
            with warnings.catch_warnings():
                warnings.simplefilter(action='ignore', category=UserWarning)

                orig_db = con._client.get_session_info(con._session).database
                con._client.switch_database(con._session, db)
                servers_list = pd.read_sql_query(f"SHOW SERVERS", con)
                con._client.switch_database(con._session, orig_db)

        for fs in server_conf['foreign_servers'][db]:
            foreign_servers_plan[db][fs] = server_conf['foreign_servers'][db][fs]

            foreign_servers_plan[db][fs]['state'] = RESOURCE_STATES.EXISTS if fs in servers_list['server_name'].values else \
                                                    RESOURCE_STATES.NEEDS_CREATION
            
            if foreign_servers_plan[db][fs]['state'] == RESOURCE_STATES.EXISTS:
                options = json.loads(servers_list[servers_list['server_name'] == fs]['options'].values[0])

                # figure out what options are different between the current
                # settings and the plan. settings can't be removed by an ALTER
                # SERVER, but they can be set to ''. hence the check for the
                # length of options[o].
                #
                fields_to_update = [ o.lower() for o in FS_OPTIONS if 
                                       o.lower() in foreign_servers_plan[db][fs] and (o not in options or len(options[o]) == 0) or
                                       o.lower() not in foreign_servers_plan[db][fs] and (o in options and len(options[0]) > 0) or
                                       o.lower() in foreign_servers_plan[db][fs] and o in options and
                                       foreign_servers_plan[db][fs][o.lower()] != options[o] 
                                   ]

                if foreign_servers_plan[db][fs]['wrapper'] != servers_list[servers_list['server_name'] == fs]['data_wrapper'].values[0]:
                    fields_to_update.append('wrapper')

                if len(fields_to_update) > 0:
                    foreign_servers_plan[db][fs]['state'] = RESOURCE_STATES.NEEDS_UPDATE
                    foreign_servers_plan[db][fs]['fields_to_update'] = fields_to_update
                else:
                    foreign_servers_plan[db][fs]['state'] = RESOURCE_STATES.UP_TO_DATE


    if len(err_msg) != 0:
        raise RuntimeError(err_msg)

    return foreign_servers_plan


def plan_foreign_tables(con: Connection, server_conf: dict, server_plan: dict) -> dict:
    foreign_tables_plan = {}
    err_msg = ''

    dbs = [ d.db_name for d in con._client.get_databases(con._session) ]

    for db in server_conf['foreign_tables']:
        if db not in dbs and \
           ('databases' not in server_plan or db not in server_plan['databases']):
            err_msg += f'    Unable to create/modify foreign tables in database "{db}": Database does not exist and not in plan.\n'
            continue

        foreign_tables_plan[db] = {}

        tab_list = []
        server_list = []

        if db in dbs:
            with warnings.catch_warnings():
                warnings.simplefilter(action='ignore', category=UserWarning)
                orig_db = con._client.get_session_info(con._session).database
                con._client.switch_database(con._session, db)
                tab_list = pd.read_sql_query(f"SHOW TABLES", con)['table_name'].values
                server_list = pd.read_sql_query(f"SHOW SERVERS", con)['server_name'].values
                con._client.switch_database(con._session, orig_db)

        for tab in server_conf['foreign_tables'][db]:
            if tab == '_comment':
                continue

            foreign_tables_plan[db][tab] = server_conf['foreign_tables'][db][tab]
            foreign_tables_plan[db][tab]['state'] = RESOURCE_STATES.EXISTS if tab in tab_list else \
                                                   RESOURCE_STATES.NEEDS_CREATION

            if 'ddl_uri' in foreign_tables_plan[db][tab] and \
               (foreign_tables_plan[db][tab]['state'] == RESOURCE_STATES.NEEDS_CREATION or \
                foreign_tables_plan[db][tab]['if_exists'] != RESOURCE_IF_EXISTS_ACTIONS.SKIP) and \
               not file_exists(foreign_tables_plan[db][tab]['ddl_uri']):

                err_msg += f'    Unable to create foreign table "{tab}" in database "{db}" from "ddl_uri": File "{foreign_tables_plan[db][tab]["ddl_uri"]}" not found.\n'
                continue

            if 'ddl_cmd' in foreign_tables_plan[db][tab]:
                ddl_cmd = foreign_tables_plan[db][tab]['ddl_cmd']
            else:
                ddl_cmd = get_file_content(foreign_tables_plan[db][tab]['ddl_uri'])
                foreign_tables_plan[db][tab]['ddl_cmd'] = ddl_cmd
            
            if not re.match(RE_IS_CREATE_FOREIGN_TABLE_DDL, ddl_cmd):
                err_msg += f'    Unable to create foreign table "{tab}" in database "{db}": DDL does not appear to be a CREATE FOREIGN TABLE statement.\n'
                continue

            if foreign_tables_plan[db][tab]['server'] not in server_list and \
               ('foreign_servers' not in server_plan or \
                db not in server_plan['foreign_servers'] or \
                foreign_tables_plan[db][tab]['server'] not in server_plan['foreign_servers'][db]):

                err_msg += f'    Unable to create foreign table "{tab}" in database "{db}": Server "{foreign_tables_plan[db][tab]["server"]}" does not exist in database "{db}" and not in plan.\n'
                continue
        

    if len(err_msg) != 0:
        raise RuntimeError(err_msg)

    return foreign_tables_plan


def plan_dashboards(con: Connection, server_conf: dict, server_plan: dict) -> dict:
    dashboards_plan = {}
    err_msg = ''

    with warnings.catch_warnings():
        warnings.simplefilter(action='ignore', category=UserWarning)
        table_list = pd.read_sql_query(f"SELECT table_name, database_name FROM tables", con)

    dbs = [ d.db_name for d in con._client.get_databases(con._session) ]

    for db in server_conf['dashboards']:
        if db not in dbs and \
           ('databases' not in server_plan or db not in server_plan['databases']):
            err_msg += f'    Unable to create/modify dashboards in database "{db}": Database does not exist and not in plan.\n'
            continue

        dashboards_plan[db] = {}

        with warnings.catch_warnings():
            warnings.simplefilter(action='ignore', category=UserWarning)
            dash_list = pd.read_sql_query(f"SELECT dashboard_name, dashboard_id FROM dashboards WHERE database_name = '{db}'", con)

        for dash in server_conf['dashboards'][db]:
            if dash == '_comment':
                continue

            dashboards_plan[db][dash] = server_conf['dashboards'][db][dash]
            dashboards_plan[db][dash]['state'] = RESOURCE_STATES.EXISTS if dash in dash_list['dashboard_name'].values else \
                                                 RESOURCE_STATES.NEEDS_CREATION

            if dashboards_plan[db][dash]['state'] == RESOURCE_STATES.EXISTS:
                dash_id = dash_list[dash_list['dashboard_name'] == dash]['dashboard_id'].values[0]
                dashboards_plan[db][dash]['dashboard_id'] = dash_id

            if not file_exists(dashboards_plan[db][dash]['dashboard_uri']):
                err_msg += f'    Unable to import dashboard "{dash}" into database "{db}": File "{dashboards_plan[db][dash]["dashboard_uri"]}" not found.\n'
                continue

            if dashboards_plan[db][dash]['state'] == RESOURCE_STATES.EXISTS:
                if is_dash_code_same(con, db, dash_id, dashboards_plan[db][dash]["dashboard_uri"]):
                    dashboards_plan[db][dash]['state'] = RESOURCE_STATES.UP_TO_DATE
                else:
                    dashboards_plan[db][dash]['state'] = RESOURCE_STATES.NEEDS_UPDATE
            
            if dashboards_plan[db][dash]['state'] != RESOURCE_STATES.UP_TO_DATE:
                try:
                    dash_state = get_file_content(dashboards_plan[db][dash]['dashboard_uri']).split('\n')[2]
                    dash_dict = json.loads(dash_state)
                except Exception as e:
                    err_msg += f'    Unable to import dashboard "{dash}" into database "{db}": Error parsing dashboard state: {e}\n'
                    continue

                for t in get_dash_table_deps(dash_dict):
                    m = re.match(r'(\w+\.)?(\w+)', t)
                    t_db = db if m.group(1) is None else m.group(1)[:-1] # immerse doesn't currently support choosing a table from a different database, but some day ...
                    t_name = m.group(2)

                    if table_list[(table_list['database_name'] == t_db) & (table_list['table_name'] == t_name)].empty and \
                       ('static_tables' not in server_plan or \
                        t_db not in server_plan['static_tables'] or \
                        t not in server_plan['static_tables'][t_db]) and \
                       ('foreign_tables' not in server_plan or \
                        t_db not in server_plan['foreign_tables'] or \
                        t not in server_plan['foreign_tables'][t_db]):

                        err_msg += f'    Unable to import dashboard "{dash}" into database "{db}": Dependent table "{t}" does not exist and not in plan.\n'

                    # TODO: validate columns in the tables.


    if len(err_msg) != 0:
        raise RuntimeError(err_msg)

    return dashboards_plan


def plan_roles(con: Connection, server_conf: dict, server_plan: dict) -> dict:

    roles_plan = {}
    err_msg = ''

    dbs = [ d.db_name for d in con._client.get_databases(con._session) ]

    with warnings.catch_warnings():
        warnings.simplefilter(action='ignore', category=UserWarning)
        roles = pd.read_sql_query("SELECT role_name FROM roles", con)['role_name'].values
        dashboards = pd.read_sql_query("SELECT dashboard_name, dashboard_id, database_name FROM dashboards", con)

    for r in server_conf['roles']:
        roles_plan[r] = {}

        roles_plan[r]['state'] = RESOURCE_STATES.EXISTS if r in roles else \
                                 RESOURCE_STATES.NEEDS_CREATION

        if 'databases' in server_conf['roles'][r]:
            roles_plan[r]['databases'] = {}

            for d in server_conf['roles'][r]['databases']:
                if d == '_comment':
                    continue

                if d not in dbs and \
                   ('databases' not in server_plan or d not in server_plan['databases']):
                    err_msg += f'    Unable to apply database permissions on database "{d}" for role "{r}": Database does not exist and not in plan.\n'
                    continue

                roles_plan[r]['databases'][d] = server_conf['roles'][r]['databases'][d]

        if 'dashboards' in server_conf['roles'][r]:
            roles_plan[r]['dashboards'] = {}

            for db in server_conf['roles'][r]['dashboards']:
                if db == '_comment':
                    continue

                if 'databases' not in server_plan or \
                   db not in server_plan['databases']:

                    err_msg += f'    Unable to apply dashboard permissions in database "{db}" for role "{r}": Database not in plan.\n'
                    continue

                roles_plan[r]['dashboards'][db] = {}

                for dash in server_conf['roles'][r]['dashboards'][db]:

                    dash_exists = not dashboards[(dashboards['database_name'] == db) & (dashboards['dashboard_name'] == dash)].empty
                    dash_in_plan = ('dashboards' in server_plan and \
                        db in server_plan['dashboards'] and \
                        dash in server_plan['dashboards'][db])

                    if not dash_exists and not dash_in_plan:
                        err_msg += f'    Unable to apply dashboard permissions for role "{r}": Dashboard "{dash}" in database "{db}" does not exist and not in plan.\n'
                        continue

                    if dash_exists and not dash_in_plan or \
                       server_plan['dashboards'][db][dash]['state'] == RESOURCE_STATES.UP_TO_DATE:

                        dash_id = get_dash_id_from_name(con, db, dash)

                    else:
                        dash_id = -1

                    if dash_id == -1:
                        dash_id = f"'{DASH_ID_TBD_PREFIX}" + dash.replace("'", "\\'") + "'"
                    roles_plan[r]['dashboards'][db][dash_id] = server_conf['roles'][r]['dashboards'][db][dash]

    if len(err_msg) != 0:
        raise RuntimeError(err_msg)

    return roles_plan


def plan_policies(con: Connection, server_conf: dict, server_plan: dict) -> dict:
    err_msg = ''
    policies_plan = {}

    with warnings.catch_warnings():
        warnings.simplefilter(action='ignore', category=UserWarning)
        users = pd.read_sql_query("SELECT user_name FROM users", con)['user_name'].values
        roles = pd.read_sql_query("SELECT role_name FROM roles", con)['role_name'].values

    dbs = [ d.db_name for d in con._client.get_databases(con._session) ]

    for db in server_conf['policies']:
        if db not in dbs and \
           ('databases' not in server_plan or db not in server_plan['databases']):
            err_msg += f'    Unable to apply policies in database "{db}": Database does not exist and not in plan.\n'
            continue

        policies_plan[db] = {}

        if db in dbs:
            con._client.switch_database(con._session, db)
            table_list = con.get_tables()
        else:
            table_list = []

        for tab in server_conf['policies'][db]:

            # the situations for setting policies on a column are complicated
            # by: 
            #   - does the table/column already exist?
            #   - is there an incoming table?
            #   - what to do if the answer to the above is "yes"
            #   - what the definition of the incoming table is
            # need to gather someG2 before we can figure out what to do.
            #
            tab_exists = False
            tab_in_plan = False
            tab_is_restore = False
            tab_if_exists_action = RESOURCE_IF_EXISTS_ACTIONS.UNDEF
            ddl_cmd = None

            if tab in table_list:
                tab_exists = True
                col_list = con.get_column_details(tab)

            if 'static_tables' in server_plan and \
               db in server_plan['static_tables'] and \
               tab in server_plan['static_tables'][db]:
                
                tab_in_plan = True
                tab_if_exists_action = server_plan['static_tables'][db][tab]['if_exists']
            
                if 'ddl_cmd' in server_plan['static_tables'][db][tab]:
                    ddl_cmd = server_plan['static_tables'][db][tab]['ddl_cmd']

                if 'import' in server_plan['static_tables'][db][tab]:
                    tab_is_restore = server_plan['static_tables'][db][tab]['import']['is_dump']

            elif 'foreign_tables' in server_plan and \
                 db in server_plan['foreign_tables'] and \
                 tab in server_plan['foreign_tables'][db]:
                
                tab_in_plan = True
                tab_if_exists_action = server_plan['foreign_tables'][db][tab]['if_exists']

                if 'ddl_cmd' in server_plan['foreign_tables'][db][tab]:
                    ddl_cmd = server_plan['foreign_tables'][db][tab]['ddl_cmd']

            if not tab_exists and not tab_in_plan:
                err_msg += f'    Unable to apply policies to table "{tab}" in database "{db}": Table does not exist and not in plan.\n'
                continue
                
            policies_plan[db][tab] = {}

            for col in server_conf['policies'][db][tab]:
                col_exists = False
                col_in_plan = False
                col_type = None

                if tab_exists and col in [c.name for c in col_list]:
                    col_exists = True
                    col_type = [ c for c in col_list if c.name == col ][0].type

                # determine the column type from the DDL command if it exists.
                #
                if ddl_cmd and not tab_is_restore:
                    if m := re.match(f'(?i)({col})' + r'\s+(' + '|'.join(COLUMN_DATATYPES.__members__.keys()) + ')', ddl_cmd):
                        col_in_plan = True
                        col_type = m.group(2)

                # not allowed situations:
                #
                if not col_exists and not col_in_plan and not tab_is_restore:
                    err_msg += f'    Unable to apply policy in database "{db}", column "{tab}.{col}": Column does not exist and not in plan.\n'
                    continue
            
                if col_exists and tab_in_plan and \
                   tab_if_exists_action != RESOURCE_IF_EXISTS_ACTIONS.SKIP and \
                   not col_in_plan and not tab_is_restore:

                    err_msg += f'    Unable to apply policy in database "{db}", column "{tab}.{col}": Column currently exists but not in planned replacement.\n'
                    continue
 
                policies_plan[db][tab][col] = {}

                for ur in server_conf['policies'][db][tab][col]:
                    policies_plan[db][tab][col][ur] = {}

                    if ('users' not in server_plan or ur not in server_plan['users']) and \
                       ('roles' not in server_plan or ur not in server_plan['roles']) and \
                       ur not in users and \
                       ur not in roles:
                
                        err_msg += f'    Unable to apply policy in database "{db}", column "{tab}.{col}": User/role "{ur}" does not exist and not in plan.\n'
                        continue

                    policies_plan[db][tab][col][ur]['state'] = RESOURCE_STATES.NEEDS_CREATION

                    if ur in users or ur in roles:
                        with warnings.catch_warnings():
                            warnings.simplefilter(action='ignore', category=UserWarning)
                            df = pd.read_sql_query(f'SHOW POLICIES {ur}', con)

                        if f'{tab}.{col}'.upper() in [ str(c).upper() for c in df['COLUMN'].values ]:
                            policies_plan[db][tab][col][ur]['state'] = RESOURCE_STATES.EXISTS

                    values = server_conf['policies'][db][tab][col][ur]

                    # make sure values are quoted properly depending on column type
                    #
                    if col_type:

                        # if the values list is a string, use the CSV library to 
                        # split the string into a list of unquoted values
                        #
                        if isinstance(values, str):
                            qc = "'" if re.match(r"^[']", values.strip()) else '"'
                            values = list(csv.reader([values], quotechar=qc, delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True))[0]

                        if col_type.upper() in ['STR', 'TEXT', 'DATE', 'TIME', 'TIMESTAMP', 'POINT', 'LINESTRING', 'POLYGON', 'MULTIPOLYGON', 'GEOMETRY', 'MULTILINESTRING', 'MULTIPOINT']:
                            values_sql = "'" + "', '".join(values) + "'"
                        else:
                            values_sql = ", ".join(values)
                    
                    # if the column type couldn't be determined (i.e. data from
                    # a dump file), then take the values as-is and hope for the
                    # best.
                    #
                    else:
                        values_sql = values if isinstance(values, str) else ", ".join(values)

                    policies_plan[db][tab][col][ur]['values'] = values_sql

    con._client.switch_database(con._session, 'information_schema')

    if len(err_msg) != 0:
        raise RuntimeError(err_msg)

    return policies_plan


def plan_users(con: Connection, server_conf: dict, server_plan: dict) -> dict:
    users_plan = {}
    err_msg = ''

    dbs = [ d.db_name for d in con._client.get_databases(con._session) ]

    with warnings.catch_warnings():
        warnings.simplefilter(action='ignore', category=UserWarning)
        roles = pd.read_sql_query("SELECT role_name FROM roles", con)['role_name'].values
        users = pd.read_sql_query("SELECT user_name FROM users", con)['user_name'].values

    for u in server_conf['users']:
        users_plan[u] = server_conf['users'][u]

        if users_plan[u]['default_db'] not in dbs and \
           users_plan[u]['default_db'] not in server_plan['databases']:

            err_msg += f'    Unable to apply default database "{users_plan[u]["default_db"]}" to user "{u}": Database does not exist and not in plan.\n'
            continue

        users_plan[u]['state'] = RESOURCE_STATES.EXISTS if u in users else \
                                 RESOURCE_STATES.NEEDS_CREATION
        
        if 'roles' in server_conf['users'][u]:
            for r in server_conf['users'][u]['roles']:
                if r not in roles and \
                   ('roles' not in server_plan or r not in server_plan['roles']):

                    err_msg += f'    Unable to apply role "{r}" to user "{u}": Role does exist and not in plan.\n'
                    continue

    if len(err_msg) != 0:
        raise RuntimeError(err_msg)

    return users_plan


def generate_ddl(plan: dict) -> list[str]:
    ddls = {}

    # generate a common postfix for all resources that are being replaced in
    # case we need to roll back the replacement(s)
    #
    new_resource_postfix = '_replaced_on_' + datetime.now().strftime('%Y%m%d%H%M%S')
    def_db = plan['default_database']

    if 'configs' in plan:
        pass

    if 'databases' in plan:
        if 'databases' not in ddls:
            ddls['databases'] = { def_db: [f'\\db {def_db}'] }
        
        for db in plan['databases']:
            if plan['databases'][db]['state'] == RESOURCE_STATES.NEEDS_CREATION:
                ddls['databases'][def_db].append(f'CREATE DATABASE {db}')
            else:
                ddls['databases'][def_db].append(f'-- {COLORS.GREEN}Database "{db}" exists. Skipping.{COLORS.END}')


    if 'static_tables' in plan:
        if 'static_tables' not in ddls:
            ddls['static_tables'] = {}
        
        for db in plan['static_tables']:
            if db not in ddls['static_tables']:
                ddls['static_tables'][db] = [f'\\db {db}']

            for tab in plan['static_tables'][db]:

                if plan['static_tables'][db][tab]['state'] == RESOURCE_STATES.EXISTS:
                    if plan['static_tables'][db][tab]['if_exists'] == RESOURCE_IF_EXISTS_ACTIONS.SKIP:
                        ddls['static_tables'][db].append(f'-- {COLORS.WARNING}Table "{tab}" exists but "if_exists" flag set to "skip". Skipping.{COLORS.END}')
                        continue
                    else:
                        # even if the plan is to replace a table, make a
                        # backup until we're sure the replacement is
                        # successful. we'll drop it later.
                        #
                        new_tab = tab + new_resource_postfix
                        ddls['static_tables'][db].append(f'ALTER TABLE {tab} RENAME TO {new_tab}')

                # The RESTORE TABLE command requires that the table being
                # restored does not already exist (unlike COPY...FROM).
                #
                if 'import' not in plan['static_tables'][db][tab] or \
                   not plan['static_tables'][db][tab]['import']['is_dump']:

                    ddl = plan['static_tables'][db][tab]['ddl_cmd']
                    
                    # make sure the ddl uses the correct table name as specified
                    # in the plan
                    #
                    ddl = re.sub(RE_IS_CREATE_STATIC_TABLE_DDL, f'CREATE TABLE {tab}', ddl)

                    ddls['static_tables'][db].append(ddl)

                if 'import' in plan['static_tables'][db][tab]:
                    ddl = ''

                    if plan['static_tables'][db][tab]['import']['is_dump']:
                        ddl = f"RESTORE TABLE {tab} FROM '{plan['static_tables'][db][tab]['import']['source_uri']}'"
                    else:
                        ddl = f"COPY {tab} FROM '{plan['static_tables'][db][tab]['import']['source_uri']}'"

                    if 'with_clause' in plan['static_tables'][db][tab]['import']:
                        with_clause = plan['static_tables'][db][tab]['import']['with_clause']

                        if with_clause.startswith('(') and with_clause.endswith(')'):
                            with_clause = with_clause[1:-1]

                        ddl += f' WITH ({with_clause})'

                    ddls['static_tables'][db].append(ddl)

                # drop the old table if it exists and the plan is to replace it
                #
                if plan['static_tables'][db][tab]['state'] == RESOURCE_STATES.EXISTS and \
                   plan['static_tables'][db][tab]['if_exists'] == RESOURCE_IF_EXISTS_ACTIONS.REPLACE:
                    ddls['static_tables'][db].append(f'DROP TABLE {new_tab}')


    if 'foreign_servers' in plan:
        if 'foreign_servers' not in ddls:
            ddls['foreign_servers'] = {}
        
        for db in plan['foreign_servers']:
            if db not in ddls['foreign_servers']:
                ddls['foreign_servers'][db] = [f'\\db {db}']

            # can only create or alter here. drop would require dropping all of
            # the dependent foreign tables, which is not something we want to
            # do, nor is it particularly easy to find out what those tables
            # are. there's no "foreign_tables" table in the information_schema
            # database nor is there a SHOW FOREIGN TABLES command.
            #
            for fs in plan['foreign_servers'][db]:
                match plan['foreign_servers'][db][fs]['state']:

                    case RESOURCE_STATES.UP_TO_DATE:
                        ddls['foreign_servers'][db].append(f'-- {COLORS.GREEN}Server "{fs}" exists and is up to date. Skipping.{COLORS.END}')
                        continue

                    case RESOURCE_STATES.NEEDS_CREATION:
                        ddl = f'CREATE SERVER {fs} FOREIGN DATA WRAPPER {plan["foreign_servers"][db][fs]["wrapper"]} WITH (' + \
                                   ', '.join([ f"{o}='{plan['foreign_servers'][db][fs][o.lower()]}'" for o in FS_OPTIONS if o.lower() in plan['foreign_servers'][db][fs] ]) + \
                               ')'
                        ddls['foreign_servers'][db].append(ddl)

                    case RESOURCE_STATES.NEEDS_UPDATE:
                        if 'wrapper' in plan['foreign_servers'][db][fs]['fields_to_update']:
                            ddl = f'ALTER SERVER {fs} SET FOREIGN DATA WRAPPER {plan["foreign_servers"][db][fs]["wrapper"]}'
                            ddls['foreign_servers'][db].append(ddl)
                            plan['foreign_servers'][db][fs]['fields_to_update'].remove('wrapper')

                        if len(plan['foreign_servers'][db][fs]['fields_to_update']) > 0:
                            ddl = f'ALTER SERVER {fs} SET (' + \
                                       ', '.join([ f"{o.upper()}='{plan['foreign_servers'][db][fs][o]}'" for o in plan['foreign_servers'][db][fs]['fields_to_update'] ]) + \
                                   ')'
                            ddls['foreign_servers'][db].append(ddl)
                
                if 'user_mapping_with_clause' in plan['foreign_servers'][db][fs]:
                    ddls['foreign_servers'][db].append(f'DROP USER MAPPING IF EXISTS FOR PUBLIC SERVER {fs}')

                    with_clause = plan['foreign_servers'][db][fs]['user_mapping_with_clause']
                    if with_clause.startswith('(') and with_clause.endswith(')'):
                        with_clause = with_clause[1:-1]

                    ddls['foreign_servers'][db].append(f'CREATE USER MAPPING FOR PUBLIC SERVER {fs} WITH ({with_clause})')


    if 'foreign_tables' in plan:
        if 'foreign_tables' not in ddls:
            ddls['foreign_tables'] = {}
        
        for db in plan['foreign_tables']:
            if db not in ddls['foreign_tables']:
                ddls['foreign_tables'][db] = [f'\\db {db}']

            for tab in plan['foreign_tables'][db]:
                ddl = plan['foreign_tables'][db][tab]['ddl_cmd']
                
                # make sure the ddl uses the correct table name and server
                # as specified in the plan
                #
                ddl = re.sub(RE_IS_CREATE_FOREIGN_TABLE_DDL, f'CREATE FOREIGN TABLE {tab}', ddl)
                ddl = re.sub(RE_IS_FOREIGN_SERVER_CLAUSE, f') SERVER {plan["foreign_tables"][db][tab]["server"]} ', ddl)
                if 'with_clause' in plan['foreign_tables'][db][tab]:
                    ddl = re.sub(RE_IS_WITH_CLAUSE, f" WITH ({plan['foreign_tables'][db][tab]['with_clause']})", ddl)

                if plan['foreign_tables'][db][tab]['state'] == RESOURCE_STATES.EXISTS:
                    if plan['foreign_tables'][db][tab]['if_exists'] == RESOURCE_IF_EXISTS_ACTIONS.SKIP:
                        ddls['foreign_tables'][db].append(f'-- {COLORS.WARNING}Table "{tab}" exists but "if_exists" flag set to "skip". Skipping.{COLORS.END}')
                        continue
                    else:
                        # even if the plan is to replace a table, make a
                        # backup until we're sure the replacement is
                        # successful. we'll drop it later.
                        #
                        new_tab = tab + new_resource_postfix
                        ddls['foreign_tables'][db].append(f'ALTER FOREIGN TABLE {tab} RENAME TO {new_tab}')

                ddls['foreign_tables'][db].append(ddl)

                # drop the old table if it exists and the plan is to replace it
                #
                if plan['foreign_tables'][db][tab]['state'] == RESOURCE_STATES.EXISTS and \
                   plan['foreign_tables'][db][tab]['if_exists'] == RESOURCE_IF_EXISTS_ACTIONS.REPLACE:
                    ddls['foreign_tables'][db].append(f'DROP FOREIGN TABLE {new_tab}')


    # none of these backslash commands are supported by the python client (and
    # except for \import_dashboard, don't exist for heavysql either), so these
    # are just placeholders for the apply() method to use to figure out what
    # python equivalents to use. There are no DDL equivalents for these, either.
    #
    if 'dashboards' in plan:
        if 'dashboards' not in ddls:
            ddls['dashboards'] = {}
        
        for db in plan['dashboards']:
            if db not in ddls['dashboards']: 
                ddls['dashboards'][db] = [f'\\db {db}']

            for dash_name in plan['dashboards'][db]:
                escaped_dn = dash_name.replace("'", "\\'")

                if plan['dashboards'][db][dash_name]['state'] == RESOURCE_STATES.UP_TO_DATE:
                    ddls['dashboards'][db].append(f'-- {COLORS.GREEN}Dashboard "{dash_name}" exists and is up to date. Skipping.{COLORS.END}')
                    continue

                elif plan['dashboards'][db][dash_name]['state'] == RESOURCE_STATES.NEEDS_UPDATE:

                    new_dn = escaped_dn + new_resource_postfix

                    if plan['dashboards'][db][dash_name]['if_exists'] == RESOURCE_IF_EXISTS_ACTIONS.REPLACE:
                        ddls['dashboards'][db].append(f'\\drop_dashboard "{escaped_dn}"')
                    elif plan['dashboards'][db][dash_name]['if_exists'] == RESOURCE_IF_EXISTS_ACTIONS.RENAME:
                        ddls['dashboards'][db].append(f'\\rename_dashboard "{escaped_dn}" "{new_dn}"')
                    else:
                        ddls['dashboards'][db].append(f'-- {COLORS.WARNING}Dashboard "{dash_name}" exists but "if_exists" flag set to "skip". Skipping.{COLORS.END}')
                        continue
                
                if plan['dashboards'][db][dash_name]['state'] == RESOURCE_STATES.NEEDS_CREATION or \
                   plan['dashboards'][db][dash_name]['if_exists'] != RESOURCE_IF_EXISTS_ACTIONS.SKIP:
                    ddls['dashboards'][db].append(f'\\import_dashboard "{escaped_dn}" "{plan["dashboards"][db][dash_name]["dashboard_uri"]}"')


    if 'roles' in plan:
        if 'roles' not in ddls:
            ddls['roles'] = { def_db: [f'\\db {def_db}'] }
        
        for r in plan['roles']:
            if plan['roles'][r]['state'] == RESOURCE_STATES.EXISTS:
                ddls['roles'][def_db].append(f'-- {COLORS.WARNING}Role "{r}" exists. Recreating.{COLORS.END}')
                ddls['roles'][def_db].append(f'DROP ROLE {r}')

            ddls['roles'][def_db].append(f'CREATE ROLE {r}')

            for db in plan['roles'][r]['databases']:
                ddls['roles'][def_db].append(f'GRANT {", ".join(plan["roles"][r]["databases"][db])} ON DATABASE {db} TO {r}')

            for db in plan['roles'][r]['dashboards']:
                if db not in ddls['roles']: 
                    ddls['roles'][db] = [f'\\db {db}']

                for dash_id in plan['roles'][r]['dashboards'][db]:
                    ddls['roles'][db].append(f'GRANT {", ".join(plan["roles"][r]["dashboards"][db][dash_id])} ON DASHBOARD {dash_id} TO {r}')


    if 'policies' in plan:
        ddls['policies'] = {}

        for db in plan['policies']:
            if db not in ddls['policies']:
                ddls['policies'][db] = [ f'\\db {db}' ]

            for tab in plan['policies'][db]:
                for col in plan['policies'][db][tab]:
                    for ur in plan['policies'][db][tab][col]:

                        # drop the policy if it exists, but only if the role or user don't already
                        # have a DROP DDL (the policy is dropped when the role or user is dropped)
                        #
                        if plan['policies'][db][tab][col][ur]['state'] == RESOURCE_STATES.EXISTS and \
                           f'DROP ROLE {ur}' not in ddls['roles'][def_db]:
                            ddls['policies'][db].append(f'DROP POLICY ON COLUMN {tab}.{col} FROM {ur}')
                        elif plan['policies'][db][tab][col][ur]['state'] == RESOURCE_STATES.EXISTS:
                            ddls['policies'][db].append(f'-- {COLORS.WARNING}Policy on column "{tab}.{col}" for "{ur}" is being recreated because role "{ur}" was recreated.{COLORS.END}')
                        
                        ddls['policies'][db].append(f'CREATE POLICY ON COLUMN {tab}.{col} TO "{ur}" VALUES ({plan["policies"][db][tab][col][ur]["values"]})')


    if 'users' in plan:
        if 'users' not in ddls:
            ddls['users'] = { def_db: [f'\\db {def_db}'] }
        
        for u in plan['users']:
            udict = plan['users'][u]
            if udict['state'] == RESOURCE_STATES.NEEDS_CREATION:
                password = udict['password'] if udict['password'] != DEFAULT_USER_INIT_PASSWORD else \
                                ''.join(random.choice(string.ascii_letters + string.digits + '!#$^*-_') for _ in range(16))
                ddls['users'][def_db].append(f"CREATE USER \"{u}\" (password='{password}', is_super='{udict['is_super']}', can_login='{udict['can_login']}', default_db='{udict['default_db']}')")

            elif udict['state'] == RESOURCE_STATES.EXISTS:
                ddls['users'][def_db].append(f"ALTER USER \"{u}\" (is_super='{udict['is_super']}', can_login='{udict['can_login']}', default_db='{udict['default_db']}')")
                

            if 'roles' in plan['users'][u]:
                for r in plan['users'][u]['roles']:
                    ddls['users'][def_db].append(f'GRANT {r} TO "{u}"')
        

    # the order of dictionary keys is not deterministic, so we need to make 
    # sure DDL's are executed in the correct order
    #
    retval = []
    for section in ('configs', 'databases', 'static_tables', 'foreign_servers', 'foreign_tables', 'dashboards', 'roles', 'policies', 'users'):
        if section in ddls:
            retval.append(f'-- ({section}) --')

            # make sure the default database DDL's are executed first
            #
            if def_db in ddls[section]:
                retval = retval + ddls[section][def_db]

            # process the rest of the databases
            #
            if len(section) > (1 if def_db in ddls[section] else 0):
                for db in ddls[section]:
                    if db == def_db:
                        continue

                    retval = retval + ddls[section][db]

    return retval
