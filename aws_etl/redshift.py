# -*- coding: utf-8 -*-

"""AWS Redshift ETL Builder"""

import logging
import psycopg2
import re
import time
import aws_etl.utils


DEFAULT_TIMESTAMP = time.strftime('%Y%m%d%H%M%S')

log = logging.getLogger(__name__)


class RedshiftETLBuilder:

    def __init__(self, session, cluster):
        self.cluster = cluster
        self.session = session
        log.debug('Getting credentials from session')
        c = self.session.get_credentials()
        self.creds = {
            'access_key': c.access_key,
            'secret_key': c.secret_key,
            'token': c.token
        }

    def _connect(self):
        if self.cluster.get('encrypted_password'):
            self.cluster['password'] = aws_etl.utils.decrypt(
                self.cluster['encrypted_password'])

        self.connection = psycopg2.connect(
            host=self.cluster['host'],
            port=self.cluster['port'],
            sslmode='require',
            user=self.cluster['user'],
            password=self.cluster['password'],
            database=self.cluster['database'])
        return self.connection

    def _get_cursor(self):
        conn = self.connection
        conn.autocommit = True
        cursor = conn.cursor()
        return cursor

    def sql_script_execute(self, script_file):
        """Executes a sql script from a file."""
        with open(script_file, 'r') as sql:
            cursor = self.cursor()
            cursor.execute(sql.read())
            self.connection.commit()

    def sql_formatter(self, query, parameters={}):
        """Format sql str by passing specific and app default parameters.
        The app default parameters are last week year(lw) and last week year(lwy)"""
        if parameters:
            log.debug('Formatting query: %s', query)
            if type(parameters) is str or type(parameters) is unicode:
                ps = eval(parameters)
            parameters.update(aws_etl.utils.default_parameters())
            query = query % parameters
        sql = re.sub(r'\s+', ' ', query)
        if "'" in sql and "\\'" not in sql:
            sql = sql.replace("'", "\\'")
        log.debug('Sql query: %s', sql)
        return sql

    def sql_executor(self, queries, ps=None):
        """Execute a series of queries allocated in a default folder within the job bucket."""
        log.info('Starting sql execution')
        for fn, sql_query in queries.items():
            if isinstance(sql_query, str):
                sql_str = sql_query
            else:
                sql_str = sql_query.getvalue()
            if sql_str:
                log.debug('Query: %s', sql_str)
                sql_string_formatted = self._sql_formatter(sql_str, ps)
                log.debug('Unloading sql to s3')
                fn = fn.split('/')[-1]
                fn = fn.split('.')[0]
                exported_file = self.bucket_path + '/' + fn
                self.rs.unload_to_s3(sql_query=sql_string_formatted, sink_bucket=exported_file)
                log.info('File unloaded: %s', exported_file)
                self.exported_files.append(self.s3_key + '/' + fn)
        log.info('Finished sql execution')
