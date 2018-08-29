# -*- coding: utf-8 -*-

"""Unit tests for AWS ETL."""

import binascii
import os
import unittest
from unittest.mock import create_autospec, MagicMock, patch

from aws_etl import utils
from aws_etl.redshift import RedshiftETLBuilder


class UtilsTest(unittest.TestCase):
    def setUp(self):
        """Set up for utils module"""
        self.test_string = "some_string"
        self.enc_string = """AQICAHhMAFZJ9ZX/P9VdKkJpgz23W10f5RPrPn78nulCycZGIAHw+TLQ8lGnMEyMCGfuDOP5AAAAaDBmBgkqhkiG9
        w0BBwagWTBXAgEAMFIGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQM3TxG3rF3V45fNe4cAgEQgCVF4kjEOh0lsOCS9pWJL0/jT8x/jkK0uLp
        0qE/ENUxMFjm/XBe2"""


    def test_decrypt_call_boto3(self):
        """Mock decrypt method and test initialise boto3 client."""
        with patch('boto3.client') as boto_mock:
            kms = boto_mock.return_value
            kms.decrypt.return_value = {'PlainText': 'some_decrypted_string'}
            result = utils.decrypt(self.enc_string)
        self.assertEqual(result,'some_decrypted_string')

    def test_encrypt_call_boto3(self):
        """Mock encrypt method and test initialise boto3 client"""
        with patch('boto3.client') as boto_mock:
            kms = boto_mock.return_value
            kms.encrypt.return_value = {'CiphertextBlob': b'some_encrypted_string'}
            result = utils.encrypt(self.test_string, 'some_key_id')
        self.assertEqual(result.strip(), binascii.b2a_base64(b'some_encrypted_string').strip())

    def test_get_parameter(self):
        """Mock boto3 client for ssm and test is initialised."""
        with patch('boto3.client') as boto_mock:
            ssm = boto_mock.return_value
            ssm.get_parameter.return_value = 'value'
            result = utils.get_parameter('some_param_key')
        self.assertEqual(result, 'value')

    def test_get_instance_region(self):
        """Test get instance region url request."""
        url = 'http://169.254.169.254/latest/dynamic/instance-identity/document'
        with patch('urllib.request.urlopen') as mock_urlopen:
            cm = MagicMock()
            cm.getcode.return_value = 200
            cm.read.return_value = b'{"region" : "some_region"}'
            cm.__enter__.return_value = cm
            mock_urlopen.return_value = cm
            result = utils.get_instance_region()
            self.assertEqual(result, 'some_region')
            mock_urlopen.assert_called_once_with(url)

    def test_set_region_name(self):
        """Test set region name set DEFAULT_REGION not none"""
        utils.set_region_name()
        self.assertIsNotNone(os.environ['DEFAULT_REGION'])


class RedshiftETLBuilderTest(unittest.TestCase):
    CLUSTER = {}
    def setUp(self):
        self.rs = RedshiftETLBuilder(self.CLUSTER)
        self.mock_rs = create_autospec(RedshiftETLBuilder, spec_set=True)

    def test_connect_is_called_and_returns_(self):
        con = self.mock_rs._connect()
        assert self.mock_rs._connect.called
        self.assertIsNotNone(con)

    @patch.object(RedshiftETLBuilder, '_connect')
    def test_cursor_calls_connect(self, mock_con):
        # test that the connect method was not called
        self.assertFalse(self.rs._connect.called)
        cur = self.rs._get_cursor()
        # test when cursor called, connect is called
        assert mock_con.called

    def test_sql_script_execute_is_called(self):
        self.assertFalse(self.mock_rs.sql_scripts_execute.called)
        self.mock_rs.sql_scripts_execute(['some/path'])
        self.mock_rs.sql_scripts_execute.assert_called_with(['some/path'])


if __name__ == '__main__':
    unittest.main()
