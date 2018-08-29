# -*- coding: utf-8 -*-

"""AWS Redshift ETL Builder"""

import logging
from psycopg2 import connect, sql
import aws_etl.utils



log = logging.getLogger(__name__)


class RedshiftETLBuilder:
    """AWS Redshift ETL Builder."""

    def __init__(self, cluster, session=None):
        self.cluster = cluster
        self.session = session

    def _connect(self):
        """Connect to redshift cluster."""
        if self.cluster.get('encrypted_password'):
            self.cluster['password'] = aws_etl.utils.decrypt(
                self.cluster['encrypted_password'])

        self.connection = connect(
            host=self.cluster['host'],
            port=self.cluster['port'],
            sslmode='require',
            user=self.cluster['user'],
            password=self.cluster['password'],
            database=self.cluster['database'])
        return self.connection

    def _get_cursor(self):
        """Get redshift cluster cursor."""
        conn = self._connect()
        conn.autocommit = True
        cursor = conn.cursor()
        return cursor

    def sql_scripts_execute(self, sql_scripts, params={}):
        """Executes sql scripts from a file."""
        ps = self.parameter_handler(params)
        log.debug('Got parameters: %s', ps)
        cursor = self._get_cursor()
        for q in sql_scripts:
            with open(q, 'r') as s:
                sql_string_formatted = s.read().format(**ps)
                cursor.execute(sql.SQL(sql_string_formatted), ps)
        self.connection.commit()
        self.connection.close()

    def parameter_handler(self, parameters={}):
        """Get app default and app specific parameters."""
        log.debug('Getting parameter handler')
        ps = aws_etl.utils.default_parameters()
        if parameters:
            if type(parameters) is str:
                parameters = eval(parameters)
            ps.update(parameters)
        return ps
