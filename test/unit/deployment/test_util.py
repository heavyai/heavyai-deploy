import boto3
import unittest

from moto import mock_aws
from unittest.mock import MagicMock, patch, mock_open

from heavydb.thrift.ttypes import TDBInfo, TDashboard

from src.deployment.util import *

class UtilTestCase(unittest.TestCase):

    bucket_name = "test-bucket"
    def setUp(self):
        os.environ['AWS_DEFAULT_REGION'] = 'us-west-1'
        os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
        os.environ['AWS_SECURITY_TOKEN'] = 'testing'
        os.environ['AWS_DEFAULT_REGION'] = 'testing'

        self.mock_aws = mock_aws()
        self.mock_aws.start()

        # you can use boto3.client("s3") if you prefer
        s3 = boto3.resource("s3")
        s3.create_bucket(Bucket=self.bucket_name, CreateBucketConfiguration={'LocationConstraint': 'us-west-1'})

    def tearDown(self):
        self.mock_aws.stop()

    def test_is_valid_name(self):
        self.assertTrue(is_valid_name("valid_name"))
        self.assertFalse(is_valid_name("1invalid_name"))
        self.assertFalse(is_valid_name("invalid name"))
        self.assertFalse(is_valid_name("invalid-name"))
        self.assertFalse(is_valid_name("invalid@name"))

    def test_is_valid_dashed_name(self):
        self.assertTrue(is_valid_dashed_name("valid-name"))
        self.assertTrue(is_valid_dashed_name("valid_name"))
        self.assertFalse(is_valid_dashed_name("invalid name"))
        self.assertFalse(is_valid_dashed_name("invalid@name"))

    def test_is_valid_email(self):
        self.assertTrue(is_valid_email("test@example.com"))
        self.assertFalse(is_valid_email("invalid email"))
        self.assertFalse(is_valid_email("test@example"))

    def test_is_valid_server_name(self):
        self.assertTrue(is_valid_server_name("valid_server"))
        self.assertFalse(is_valid_server_name("default"))
        self.assertFalse(is_valid_server_name("system"))
        self.assertFalse(is_valid_server_name("internal"))

    def test_parse_ddl_args(self):
        self.assertEqual(parse_ddl_args('arg1 "arg2" arg3'), ['arg1', '"arg2"', 'arg3'])
        self.assertEqual(parse_ddl_args('arg1 "arg2 with spaces" arg3'), ['arg1', '"arg2 with spaces"', 'arg3'])
        self.assertEqual(parse_ddl_args('arg1 "arg2\\"with\\"quotes" arg3'), ['arg1', '"arg2\\"with\\"quotes"', 'arg3'])

    def test_replace_env_vars(self):
        os.environ["VAR1"] = "value1"
        os.environ["VAR2"] = "value2"
        self.assertEqual(replace_env_vars("This is $VAR1 and $VAR2"), "This is value1 and value2")
        self.assertRaises(RuntimeError, replace_env_vars, "This is $VAR3")

    def test_obfuscate_secrets(self):
        self.assertEqual(obfuscate_secrets("heavyai://admin:this_is_a_password@localhost:6274/heavyai"), "heavyai://admin:XXXX@localhost:6274/heavyai")
        self.assertEqual(obfuscate_secrets(";password=secret"), ";password=XXXX")
        self.assertEqual(obfuscate_secrets("s3_secret_key='1234567890abcdef1234567890abcdef12345678'"), "s3_secret_key='1234...5678'")

    def test_resource_exists(self):
        # Mock the requests.head function to return a status code of 200
        with patch('requests.head') as mock_head:
            mock_head.return_value.status_code = 200
            self.assertTrue(resource_exists("http://example.com"))
        
        # Mock the requests.head function to return a status code of 302 and a redirect URL
        with patch('requests.head') as mock_head:
            mock_head.return_value.status_code = 302
            mock_head.return_value.headers = {'Location': '/redirect'}
            self.assertEqual(resource_exists("http://example.com", chase_redirect=True), (302, "http://example.com/redirect"))
        
        # Mock the requests.head function to return a status code of 404
        with patch('requests.head') as mock_head:
            mock_head.return_value.status_code = 404
            self.assertFalse(resource_exists("http://example.com/bob"))

    def test_file_exists(self):
        # Mock the os.path.isfile function to return True
        with patch('os.path.isfile') as mock_isfile:
            mock_isfile.return_value = True
            self.assertTrue(file_exists("./test_util.py"))
        
        # Mock the os.path.isfile function to return False
        with patch('os.path.isfile') as mock_isfile:
            mock_isfile.return_value = False
            self.assertFalse(file_exists("/path/to/non_existent_file"))

    def test_get_file_content_from_url(self):
        # Mock the requests.get function to return a response with text content
        with patch('requests.get') as mock_get:
            mock_get.return_value.text = "This is the file content"
            self.assertEqual(get_file_content_from_url("http://example.com/file.txt"), "This is the file content")
        
        # Use the mock s3 env (see setUp method) to return a response with body content
        content = "This is the file content"
        key = "file.txt"
        s3 = boto3.client("s3")
        s3.put_object(Bucket=self.bucket_name, Key=key, Body=content)

        self.assertEqual(get_file_content_from_url(f"s3://{self.bucket_name}/{key}", s3_client=s3), content)

    def test_get_file_content(self):
        # Mock the get_file_content function to return file content
        with patch('os.path.isfile') as mock_isfile:
            mock_isfile.return_value = True
            with patch("builtins.open", mock_open(read_data="This is the file content")) as mock_file:
                self.assertEqual(get_file_content("/path/to/file"), "This is the file content")
        
        # Mock the os.path.isfile function to return False
        with patch('os.path.isfile') as mock_isfile:
            mock_isfile.return_value = False
            self.assertRaises(RuntimeError, get_file_content, "/path/to/file")

    def test_is_dash_code_same(self):
        # Mock the Connection and Client objects
        con = MagicMock()
        con._client.get_session_info.return_value.database = "test_db"
        con._client.get_dashboard.return_value.dashboard_state = base64.b64encode(b"dashboard_state")
        with patch('src.deployment.util.get_file_content') as mock_get_file_content:
            mock_get_file_content.return_value = 'line1\nline2\ndashboard_state'
            self.assertTrue(is_dash_code_same(con, "test_db", 1, "/path/to/dashboard"))

    def test_get_dash_id_from_name(self):
        # Mock the Connection and Client objects
        con = MagicMock()
        con._client.get_databases.return_value = [TDBInfo(db_name='test_db', db_owner='admin')]
        con._client.get_dashboards.return_value = [TDashboard(dashboard_id=1, dashboard_name='test_dashboard')]
        self.assertEqual(get_dash_id_from_name(con, "test_db", "test_dashboard"), 1)
        self.assertEqual(get_dash_id_from_name(con, "test_db", "nonexistent_dashboard"), -1)

    # def test_exec_dash_ddl(self):
    #     # Mock the Connection and Client objects
    #     con = MagicMock()
    #     con._client.get_session_info.return_value.database = "test_db"
    #     con._client.get_dashboards.return_value = [TDashboard(dashboard_id=1, dashboard_name='test_dashboard')]
    #     con._client.create_dashboard.return_value = None
    #     con.duplicate_dashboard.return_value = None
    #     con._client.delete_dashboard.return_value = None
    #     self.assertRaises(RuntimeError, exec_dash_ddl, con, '\\invalid_command')
    #     exec_dash_ddl(con, '\\drop_dashboard "test_dashboard"')
    #     exec_dash_ddl(con, '\\rename_dashboard "test_dashboard" "new_dashboard"')
    #     exec_dash_ddl(con, '\\import_dashboard "test_dashboard" "/path/to/dashboard"')

    def test_get_dash_table_deps_from_db(self):
        # Mock the Connection and Client objects
        con = MagicMock()
        con._client.get_session_info.return_value.database = "test_db"
        con._client.get_dashboard.return_value.dashboard_state = base64.b64encode(b'{"dashboard_state": "1"}')
        with patch('src.deployment.util.get_dash_table_deps') as mock_get_dash_table_deps:
            mock_get_dash_table_deps.return_value = ['table1', 'table2']
            self.assertEqual(get_dash_table_deps_from_db(con, "test_db", 1), ['table1', 'table2'])

    def test_get_dash_table_deps(self):
        dash = {
            'tabs': {
                'tab1': {
                    'dashboard': {
                        'dataSources': {
                            'table1': {},
                            '${-N1}': {},
                            '${-N2}': {}
                        }
                    }
                },
                'tab2': {
                    'dashboard': {
                        'dataSources': {
                            'table2': {},
                            '${-N3}': {}
                        }
                    }
                }
            },
            'parameters': {
                'definitions': {
                    '-N1': {'defaultValue': '"table3" JOIN "table4"', 'type': 'JOIN'},
                    '-N2': {'defaultValue': '"table5" JOIN "table6"', 'type': 'JOIN'},
                    '-N3': {'defaultValue': 'SELECT * FROM "table7"', 'type': 'TABLE'}
                }
            }
        }
        # should not pick up "table7" at this time as it's a custom data source
        # and there isn't an easy way to parse the SELECT and extract tables at
        # this time.
        self.assertEqual(get_dash_table_deps(dash), ['table1', 'table2', 'table3', 'table4', 'table5', 'table6'])

if __name__ == '__main__':
    unittest.main()