import re

from enum import Enum

VERBOSE_WIDTH = 80 # width of verbose output

class COLORS:
    MAGENTA = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'
    HEADER = BLUE + BOLD
    WARNING = YELLOW
    FAIL = RED

class RESOURCE_STATES(Enum):
    EXISTS = 0
    UP_TO_DATE = 1
    NEEDS_CREATION = 2
    NEEDS_UPDATE = 3
    NEEDS_DELETION = 4
    SKIP = 5

class ARG_PARSE_STATES(Enum):
    SPACE = 0
    WORD = 1
    QUOTE = 2

class RESOURCE_IF_EXISTS_ACTIONS(Enum):
    UNDEF = -1
    SKIP = 0
    RENAME = 1
    REPLACE = 2

class COLUMN_DATATYPES(Enum):
    SMALLINT = 0
    INTEGER = 1
    BIGINT = 2
    FLOAT = 3
    DECIMAL = 4
    DOUBLE = 5
    TEXT = 6
    TIME = 7
    TIMESTAMP = 8
    DATE = 9
    BOOLEAN = 10
    POINT = 13
    LINESTRING = 14
    POLYGON = 15
    MULTIPOLYGON = 16
    TINYINT = 17
    GEOMETRY = 18
    MULTILINESTRING = 20
    MULTIPOINT = 21
    EPOCH = 22
    INT = 23

# foreign server data wrapper names
#
FS_DATA_WRAPPERS = [
    'delimited_file',
    'odbc',
    'parquet_file',
    'regex_parsed_file'
]

# foreign server options
#
FS_OPTIONS = [
    'STORAGE_TYPE',
    'BASE_PATH',
    'S3_BUCKET',
    'AWS_REGION',
    # 'S3_ENDPOINT', # not supported until it's determined how this really works
    'DATA_SOURCE_NAME',
    'CONNECTION_STRING'
]

# foreign server storage types
#
FS_STORAGE_TYPES = [
    'LOCAL_FILE',
    'AWS_S3'
]

DASH_ID_TBD_PREFIX = 'DASH_ID_TBD_'

# security constants. these are not by any means complete, but cover what's
# needed for HeavyEco.
#
PRIVS_DATABASE = [
    'ALL', 'ACCESS', 'SELECT', 'VIEW DASHBOARD', 'VIEW SQL EDITOR'
]

PRIVS_DASHBOARD = [
    'VIEW'
]

# maps GRANT command privileges to heavydb permissions in the 
# information_schema.permissions table. again, not complete, but
# covers what's needed for HeavyEco.
#
PRIVS_TO_PERMS_MAP = {
    'database': {
        'ALL': { 'object_type': 'database', 'permission': 'all' },
        'ACCESS': { 'object_type': 'database', 'permission': 'access' },
        'SELECT': { 'object_type': 'table', 'permission': 'select table' }, # object_id = -1
        'VIEW DASHBOARD': { 'object_type': 'dashboard', 'permission': 'view dashboard' }, # object_id = -1
        'VIEW SQL EDITOR': { 'object_type': 'database', 'permission': 'view_sql_editor' }
    },
    'dashboard': {
        'VIEW': { 'object_type': 'dashboard', 'permission': 'view dashboard' } # object_id = <dashboard_id>
    }
}

# regular expressions for validating json inputs
#
RE_IS_TRUE = re.compile(r'(?i)^(y|yes|true|t|1)$')
RE_IS_DB_URL = re.compile(r'^heavyai://[^:@/]+:[^:@/]+@[^:@/]+(:\d+)?/\w+$')
RE_IF_EXISTS_RESOURCE = re.compile('(?i)^(' + '|'.join(RESOURCE_IF_EXISTS_ACTIONS.__members__.keys()) + ')$')
RE_INVALID_SERVER_NAMES = re.compile(r'(?i)^(?:default|system|internal).*$')
RE_DATA_WRAPPERS = re.compile('(?i)^(' + '|'.join(FS_DATA_WRAPPERS) + ')$')
RE_STORAGE_TYPES = re.compile('(?i)^(' + '|'.join(FS_STORAGE_TYPES) + ')$')
RE_PRIVS_DATABASE = re.compile('(?i)^(' + '|'.join(PRIVS_DATABASE) + ')$')
RE_PRIVS_DASHBOARD = re.compile('(?i)^(' + '|'.join(PRIVS_DASHBOARD) + ')$')

# regular expressions for parsing DDL statements
#
RE_IS_VALID_NAME = re.compile(r'^[a-zA-Z][a-zA-Z0-9\$_]*$')
RE_IS_VALID_DASHED_NAME = re.compile(r'^[A-Za-z_][A-Za-z0-9\$_\-]*$')
RE_IS_VALID_EMAIL = re.compile(r'^([^\s\"]+|\".+\")@[A-Za-z0-9][A-Za-z0-9\-\.]*\.[A-Za-z]+$')
RE_IS_DASH_MGMT_DDL = re.compile(r'(?i)^\s*\\(drop|rename|import)_dashboard\s+')
RE_IS_GRANT_ON_DASH_ID_TBD_DDL = re.compile(r"(?i)^\s*grant\s+.*?\s+on\s+dashboard\s+('" + DASH_ID_TBD_PREFIX + r"(.+?)') to \w+$")
RE_IS_CREATE_STATIC_TABLE_DDL = r'(?i)^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[a-zA-Z][a-zA-Z0-9\$_]*'
RE_IS_CREATE_FOREIGN_TABLE_DDL = r'(?i)^\s*CREATE\s+FOREIGN\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[a-zA-Z][a-zA-Z0-9\$_]*'
RE_IS_FOREIGN_SERVER_CLAUSE = re.compile(r'(?i)\)\s+SERVER\s+([a-zA-Z][a-zA-Z0-9\$_]*)\s+')
RE_IS_WITH_CLAUSE = re.compile(r'(?i)\s+(?:WITH\s+\((.+)\)|XXWITH_CLAUSEXX);?$')
RE_IS_IMPORT_DDL = re.compile(r'(?i)^\s*(?:RESTORE\s+TABLE|COPY)\s+')
RE_IS_DROP_TABLE_DDL = re.compile(r'(?i)^\s*DROP\s+(?:FOREIGN\s+)?TABLE\s+')
RE_IS_TAB_COLUMN = re.compile('(?i)([a-zA-Z][a-zA-Z0-9\$_]*)\s+(' + '|'.join(COLUMN_DATATYPES.__members__.keys()) + ')')

# password obfuscation
#
RE_OBFUSCATE_DB_URL_PW = re.compile(r':[^:\'\s\"@]+@')
RE_OBFUSCATE_DB_URL_PW_REPL = ':XXXX@'
RE_OBFUSCATE_ODBC_PW = re.compile(r'(?i);pwd=[^;\s]+')
RE_OBFUSCATE_ODBC_PW_REPL = ';pwd=XXXX'
RE_OBFUSCATE_S3_KEY = re.compile(r"(?i)(?:s3_secret_key)\s*=\s*\'([^\']{40})\'")
RE_OBFUSCATE_S3_KEY_REPL = 'XXXX'

# connection defaults
#
DEFAULT_USER = 'admin'
DEFAULT_PASSWORD = 'HyperInteractive'
DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 6274
DEFAULT_DATABASE = 'heavyai'
DEFAULT_DB_URL = f'heavyai://{DEFAULT_USER}:{DEFAULT_PASSWORD}@{DEFAULT_HOST}:{DEFAULT_PORT}/{DEFAULT_DATABASE}'

# table definition defaults
#
DEFAULT_STATIC_TABLE_IF_EXISTS = RESOURCE_IF_EXISTS_ACTIONS.SKIP
DEFAULT_FOREIGN_TABLE_IF_EXISTS = RESOURCE_IF_EXISTS_ACTIONS.SKIP

# foreign server definition defaults
#
DEFAULT_SERVER_IF_EXISTS = RESOURCE_IF_EXISTS_ACTIONS.SKIP

# dashboard definition defaults
#
DEFAULT_DASHBOARD_IF_EXISTS = RESOURCE_IF_EXISTS_ACTIONS.RENAME

# user definition defaults
#
DEFAULT_USER_INIT_PASSWORD='<random>' # a flag to be picked up by the planner to generate a random password
DEFAULT_USER_CAN_LOGIN='true'
DEFAULT_USER_IS_SUPER='false'
DEFAULT_USER_DATABASE='heavyai'
