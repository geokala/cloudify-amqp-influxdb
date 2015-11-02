########
# Copyright (c) 2014 GigaSpaces Technologies Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
############

from copy import copy
import ssl
import sys
import unittest

from mock import patch, Mock

import amqp_influxdb


AMQP_EXCHANGE = 'test'
AMQP_ROUTING_KEY = 'test'
INFLUX_DATABASE = 'test'
REQUIRED_MAIN_ARGS = {
    '--amqp-exchange': AMQP_EXCHANGE,
    '--amqp-routing-key': AMQP_ROUTING_KEY,
    '--influx-database': INFLUX_DATABASE,
}


class ArgparseFaker(object):
    """
        Context for setting argparse arguments.
    """
    def __init__(self, args_values):
        self.args_values = args_values

    def __enter__(self):
        self.old_sys_argv = sys.argv
        sys.argv = [self.old_sys_argv[0]]
        for arg, value in self.args_values.items():
            sys.argv.append(arg)
            sys.argv.append(value)

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.argv = self.old_sys_argv
        return False


class TestSecurity(unittest.TestCase):

    @patch('amqp_influxdb.pika.BlockingConnection')
    @patch('amqp_influxdb.pika.ConnectionParameters')
    @patch('amqp_influxdb.pika.credentials.PlainCredentials')
    def test_amqp_consumer_uses_default_creds_with_no_inputs(self,
                                                             mock_creds,
                                                             mock_params,
                                                             mock_conn):
        amqp_influxdb.AMQPTopicConsumer(
            exchange='test',
            routing_key='test',
            message_processor=Mock(),
        )

        mock_creds.assert_called_once_with(
            username='guest',
            password='guest',
        )

    @patch('amqp_influxdb.pika.BlockingConnection')
    @patch('amqp_influxdb.pika.ConnectionParameters')
    @patch('amqp_influxdb.pika.credentials.PlainCredentials')
    def test_amqp_consumer_uses_default_creds_with_inputs(self,
                                                          mock_creds,
                                                          mock_params,
                                                          mock_conn):
        conn_parameters = {
            'host': 'localhost',
            'port': 5672,
            'connection_attempts': 12,
            'retry_delay': 5,
        }

        amqp_influxdb.AMQPTopicConsumer(
            exchange='test',
            routing_key='test',
            message_processor=Mock(),
            connection_parameters=conn_parameters,
        )

        mock_creds.assert_called_once_with(
            username='guest',
            password='guest',
        )

    @patch('amqp_influxdb.pika.BlockingConnection')
    @patch('amqp_influxdb.pika.ConnectionParameters')
    @patch('amqp_influxdb.pika.credentials.PlainCredentials')
    def test_amqp_consumer_uses_default_creds_with_none_creds(self,
                                                              mock_creds,
                                                              mock_params,
                                                              mock_conn):
        """
            Confirm default creds used if defaults removed from main args.
        """
        conn_parameters = {
            'host': 'localhost',
            'port': 5672,
            'connection_attempts': 12,
            'retry_delay': 5,
            'credentials': {
                'username': None,
                'password': None,
            },
        }

        amqp_influxdb.AMQPTopicConsumer(
            exchange='test',
            routing_key='test',
            message_processor=Mock(),
            connection_parameters=conn_parameters,
        )

        mock_creds.assert_called_once_with(
            username='guest',
            password='guest',
        )

    @patch('amqp_influxdb.pika.BlockingConnection')
    @patch('amqp_influxdb.pika.ConnectionParameters')
    @patch('amqp_influxdb.pika.credentials.PlainCredentials')
    def test_amqp_consumer_uses_supplied_credentials(self,
                                                     mock_creds,
                                                     mock_params,
                                                     mock_conn):
        conn_parameters = {
            'host': 'localhost',
            'port': 5672,
            'connection_attempts': 12,
            'retry_delay': 5,
            'credentials': {
                'username': 'myusername',
                'password': 'mypassword',
            },
        }

        amqp_influxdb.AMQPTopicConsumer(
            exchange='test',
            routing_key='test',
            message_processor=Mock(),
            connection_parameters=conn_parameters,
        )

        mock_creds.assert_called_once_with(
            username='myusername',
            password='mypassword',
        )

    @patch('amqp_influxdb.pika.BlockingConnection')
    @patch('amqp_influxdb.pika.ConnectionParameters')
    @patch('amqp_influxdb.pika.credentials.PlainCredentials')
    def test_amqp_consumer_no_ssl_by_default(self,
                                             mock_creds,
                                             mock_params,
                                             mock_conn):
        amqp_influxdb.AMQPTopicConsumer(
            exchange='test',
            routing_key='test',
            message_processor=Mock(),
        )

        # We don't care about most of the args but there should only have been
        # one call, with no ssl set
        self.assertEqual(mock_params.call_count, 1)
        _, params_kwargs = mock_params.call_args
        self.assertNotIn('ssl', params_kwargs)

    @patch('amqp_influxdb.pika.BlockingConnection')
    @patch('amqp_influxdb.pika.ConnectionParameters')
    @patch('amqp_influxdb.pika.credentials.PlainCredentials')
    def test_amqp_consumer_ssl_enabled(self,
                                       mock_creds,
                                       mock_params,
                                       mock_conn):
        cert_path = '/not/real/cert.pem'
        conn_parameters = {
            'host': 'localhost',
            'port': 5672,
            'connection_attempts': 12,
            'retry_delay': 5,
            'credentials': {
                'username': 'myusername',
                'password': 'mypassword',
            },
            'ssl': True,
            'ca_path': cert_path,
        }

        amqp_influxdb.AMQPTopicConsumer(
            exchange='test',
            routing_key='test',
            message_processor=Mock(),
            connection_parameters=conn_parameters,
        )

        # We don't care about most of the args but there should only have been
        # one call, with ssl configured
        self.assertEqual(mock_params.call_count, 1)
        _, params_kwargs = mock_params.call_args

        # We really want this to be True. assertTrue accepts Truthy values,
        # but we shouldn't rely on pika also accepting truthy values.
        self.assertEqual(params_kwargs['ssl'], True)

        # The ssl options should be correctly set
        self.assertNotIn('ca_path', params_kwargs)
        self.assertEqual(
            params_kwargs['ssl_options'],
            {
                'cert_reqs': ssl.CERT_REQUIRED,
                'ca_certs': cert_path,
            },
        )

    @patch('amqp_influxdb.__main__.InfluxDBPublisher')
    @patch('amqp_influxdb.__main__.AMQPTopicConsumer')
    def test_main_defaults_to_no_security(self,
                                          mock_amqp_consumer,
                                          mock_influxdb_pub):
        faker = ArgparseFaker(
            REQUIRED_MAIN_ARGS,
        )

        with faker:
            amqp_influxdb.__main__.main()

        expected_params = {
            'host': 'localhost',
            'port': amqp_influxdb.BROKER_PORT_NO_SSL,
            'connection_attempts': 12,
            'retry_delay': 5,
            'credentials': {
                'username': 'guest',
                'password': 'guest',
            },
            'ca_path': '',
            'ssl': False,
        }

        # We don't care about most of the args but there should only have been
        # one call, with ssl configured
        self.assertEqual(mock_amqp_consumer.call_count, 1)

        _, amqp_consumer_kwargs = mock_amqp_consumer.call_args
        self.assertEqual(
            amqp_consumer_kwargs['connection_parameters'],
            expected_params,
        )

    @patch('amqp_influxdb.__main__.InfluxDBPublisher')
    @patch('amqp_influxdb.__main__.AMQPTopicConsumer')
    def test_main_passes_expected_security_args(self,
                                                mock_amqp_consumer,
                                                mock_influxdb_pub):
        amqp_user = 'kilroy'
        amqp_pass = 'woshere'
        cert_path = '/not/real/cert.pem'
        cli_args = copy(REQUIRED_MAIN_ARGS)
        cli_args.update({
            '--amqp-username': amqp_user,
            '--amqp-password': amqp_pass,
            '--amqp-ssl-enabled': 'true',
            '--amqp-ca-cert-path': cert_path,
        })

        faker = ArgparseFaker(
            cli_args,
        )

        with faker:
            amqp_influxdb.__main__.main()

        expected_params = {
            'host': 'localhost',
            'port': amqp_influxdb.BROKER_PORT_SSL,
            'connection_attempts': 12,
            'retry_delay': 5,
            'credentials': {
                'username': amqp_user,
                'password': amqp_pass,
            },
            'ca_path': cert_path,
            'ssl': True,
        }

        # We don't care about most of the args but there should only have been
        # one call, with ssl configured
        self.assertEqual(mock_amqp_consumer.call_count, 1)

        _, amqp_consumer_kwargs = mock_amqp_consumer.call_args
        self.assertEqual(
            amqp_consumer_kwargs['connection_parameters'],
            expected_params,
        )
