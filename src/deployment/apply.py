from heavyai import Connection
from icecream import ic

from .constants import *
from .util import exec_dash_ddl, get_dash_id_from_name, obfuscate_secrets

def apply_ddl(con: Connection, ddls: list[str], verbose = False) -> None:
    db = None
    prev_success = False

    for ddl in ddls:
        
        if ddl.startswith('--'):
            if ddl.startswith('-- ('):
                print(f'  {COLORS.HEADER}{ddl}{COLORS.END}')
            else:
                print(f'    {ddl}')
            continue

        if ddl.startswith('\\db '):
            db = ddl.split()[1]
            print(f'    -- Switching to database "{db}" --')
            con._client.switch_database(con._session, db)
            continue

        if m := re.match(RE_IS_GRANT_ON_DASH_ID_TBD_DDL, ddl):
            full_quoted_dash = m.group(1)
            dash_name = m.group(2).replace("\\'", "'")
            dash_id = get_dash_id_from_name(con, db, dash_name)
            ddl = re.sub(full_quoted_dash, str(dash_id), ddl)

        if re.match(RE_IS_DROP_TABLE_DDL, ddl) and not prev_success:
            # skip dropping the backup table if the previous command (a "copy
            # ... from" or "restore table") errored out.
            #
            print(f'    -- {COLORS.WARNING}Previous import command failed. Skipping {ddl}{COLORS.END}')
            continue

        
        pr_ddl = obfuscate_secrets(ddl)
        if not verbose and len(pr_ddl) > VERBOSE_WIDTH:
            print(f'    {pr_ddl[:VERBOSE_WIDTH]} ...')
        else:
            print(f'    {pr_ddl}')

        if re.match(RE_IS_DASH_MGMT_DDL, ddl):
            exec_dash_ddl(con, ddl)
        else:
            try:
                con.execute(ddl)
                prev_success = True
            except Exception as e:
                if re.match(RE_IS_IMPORT_DDL, ddl):
                    # ignore import errors but note the failure for the next
                    # ddl command (which might be a "drop table" ddl)
                    #
                    print(f'    {COLORS.FAIL}-- Import failure: {e}{COLORS.END}')
                    prev_success = False
                else:
                    print(f'    {COLORS.FAIL}-- Error: {e}{COLORS.END}')
                    raise e
