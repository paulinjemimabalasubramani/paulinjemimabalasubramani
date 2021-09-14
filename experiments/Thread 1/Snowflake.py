# Databricks notebook source
__author__ = "Jeffrey Planes"
__copyright__ = "Copyright 2021, Sense Corp"
__license__ = "Commercial"
__email__ = "jplanes@sensecorp.com"

import sys
import os
import json
import contextlib

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

import snowflake.connector as snowflake
from snowflake.connector import connect
from snowflake.connector import DictCursor

from snowflake.sqlalchemy import URL
from sqlalchemy import *

class Snowflake:
    def __init__(self, job_name, secrets):
        self.jobName = job_name
        self.secrets = secrets

        ##DECLARATION SECTION
        function_id = sys._getframe().f_code.co_name

        logger.info(f"Inside method: {function_id}")
        logger.info(f"Job Name: {job_name}")

        try:

            if len(self.secrets) == 6:

                self.account = self.secrets['snowflake_account']
                self.user = self.secrets['snowflake_user']
                self.password = self.secrets['snowflake_password']
                self.dbname = self.secrets['snowflake_dbname']
                self.warehouse = self.secrets['snowflake_warehouse']
                self.role = self.secrets['snowflake_role']

            else:
                exception_message = f"Exception Occurred Inside this method {function_id} --> Wrong number of metadata key/value in the secret json"
                raise Exception(exception_message)

        except Exception as e:
            msg = e
            exception_message = f"Exception Occurred Inside this method {function_id} --> Here is the exception Message {msg}"
            logger.error(exception_message)
            raise Exception(exception_message)

    def connect(self):

        ## DECLARATION SECTION
        function_id = sys._getframe().f_code.co_name
        logger.info(f"Inside method: {function_id}")

        try:

            connection = connect(
                user=self.user,
                password=self.password,
                account=self.account,
                database=self.dbname,
                warehouse=self.warehouse,
                role=self.role,
                autocommit=True,
            )

            return connection

        except Exception as e:
            msg = e
            exception_message = f"Exception Occurred Inside this method {function_id} --> Here is the exception Message {msg}"
            logger.error(exception_message)
            raise Exception(exception_message)

    def execute_sql(self, sql, parameter=None):

        ## DECLARATION SECTION
        function_id = sys._getframe().f_code.co_name
        logger.info(f"Inside method: {function_id}")

        try:

            with contextlib.closing(self.connect()) as connection:
                with contextlib.closing(connection.cursor()) as cursor:
                    if parameter is None:
                        cursor.execute(sql)
                    else:
                        cursor.execute(sql, parameter)
                    cursor.close()

        except Exception as e:
            msg = e
            exception_message = f"Exception Occurred Inside this method {function_id} --> Here is the exception Message {msg}"
            logger.error(exception_message)
            raise Exception(exception_message)

    def execute_snowflake_string(self,snowflake_sql):

        ## DECLARATION SECTION
        function_id = sys._getframe().f_code.co_name
        logger.info(f"Inside method: {function_id}")

        try:

            with contextlib.closing(self.connect()) as connection:
                with contextlib.closing(connection.cursor()) as cursor:
                        connection.execute_string(snowflake_sql)
                cursor.close()

        except Exception as e:
            msg = e
            exception_message = f"Exception Occurred Inside this method {function_id} --> Here is the exception Message {msg}"
            logger.error(exception_message)
            raise Exception(exception_message)

    def execute_snowflake_stream(self,snowflake_file):

        ## DECLARATION SECTION
        function_id = sys._getframe().f_code.co_name
        logger.info(f"Inside method: {function_id}")

        try:

            with contextlib.closing(self.connect()) as connection:
                ##with contextlib.closing(connection.cursor()) as cursor:
                with open(snowflake_file, 'r', encoding='utf-8') as snowflake_sql:
                    for cur in connection.execute_stream(snowflake_sql):
                        for ret in cur:
                                logger.info(ret)
                cur.close()

        except Exception as e:
            msg = e
            exception_message = f"Exception Occurred Inside this method {function_id} --> Here is the exception Message {msg}"
            logger.error(exception_message)
            raise Exception(exception_message)
            
    def get_column_header(self, sql):
        """Get column headers from a given query"""
        ## DECLARATION SECTION
        functionID = sys._getframe().f_code.co_name
        logger.info(f"Inside method: {functionID}")

        try:

            with contextlib.closing(self.connect()) as connection:
                with contextlib.closing(connection.cursor()) as cursor:
                    cursor.execute(sql)
                    #  Get the fields name (only once!)
                    columnHeader = [field[0] for field in cursor.description]
                return columnHeader

        except Exception as e:
            msg = e.args
            exceptionMessage = f"Exception Occurred Inside this method {functionID} --> Here is the exception Message {msg}"
            logger.error(exceptionMessage)
            raise Exception(exceptionMessage)

    def return_dataset(self, sql, parameter=None):

        ## DECLARATION SECTION
        function_id = sys._getframe().f_code.co_name
        logger.info(f"Inside method: {function_id}")

        try:
          
            with contextlib.closing(self.connect()) as connection:
                with contextlib.closing(connection.cursor(DictCursor)) as cursor:
                    if parameter is None:
                        cursor.execute(sql)
                    else:
                        cursor.execute(sql, parameter)
                    rows = cursor.fetchall()
                return rows

        except Exception as e:
            msg = e
            exception_message = f"Exception Occurred Inside this method {function_id} --> Here is the exception Message {msg}"
            logger.error(exception_message)
            raise Exception(exception_message)




# %%

# qa-snowflake-id
# qa-snowflake-pass

