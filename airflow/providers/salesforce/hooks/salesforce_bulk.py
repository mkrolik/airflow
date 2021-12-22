#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""
This module contains a Salesforce Hook which allows you to connect to your Salesforce instance,
retrieve data from it, and write that data to a file for other uses.

.. note:: this hook also relies on the simple_salesforce package:
      https://github.com/simple-salesforce/simple-salesforce
"""
import logging
import sys
import time
from urllib.parse import urlparse
from typing import Any, Dict, Iterable, List, Optional

if sys.version_info >= (3, 8):
    from functools import cached_property
else:
    from cached_property import cached_property

from requests import Session
from salesforce_bulk import SalesforceBulk

from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)


class SalesforceBulkHook(BaseHook):
    """
    Creates new connection to Salesforce and allows you to use the Bulk API to pull data out of SFDC and push data to SFDC.

    You can then use that file with other Airflow operators to move the data from/into another data source.

    :param conn_id: The name of the connection that has the parameters needed to connect to Salesforce.
        The connection should be of type `Salesforce`.
    :type conn_id: str
    :param session_id: The access token for a given HTTP request session.
    :type session_id: str
    :param session: A custom HTTP request session. This enables the use of requests Session features not
        otherwise exposed by `simple_salesforce`.
    :type session: requests.Session

    .. note::
        A connection to Salesforce Bulk can be created via several authentication options:

        * Password: Provide Username, Password, and Security Token
        * Direct Session: Provide a `session_id` and either Instance or Instance URL

        If in sandbox, enter a Domain value of 'test'.
    """

    conn_name_attr = "salesforce_conn_id"
    default_conn_name = "salesforce_default"
    conn_type = "salesforce"
    hook_name = "Salesforce"

    def __init__(
        self,
        salesforce_conn_id: str = default_conn_name,
        session_id: Optional[str] = None,
        session: Optional[Session] = None,
    ) -> None:
        super().__init__()
        self.conn_id = salesforce_conn_id
        self.session_id = session_id
        self.session = session


    @cached_property
    def conn(self) -> SalesforceBulk:
        """Returns a Salesforce Bulk instance. (cached)"""
        connection = self.get_connection(self.conn_id)
        extras = connection.extra_dejson
        # all extras below (besides the apiversion and sandbox) are explicitly defaulted to None
        # because salesforcebulk has a built-in authentication-choosing method that
        # relies on which arguments are None and without "or None" setting this connection
        # in the UI will result in the blank extras being empty strings instead of None,
        # which would break the connection.
        conn = SalesforceBulk(
            sessionId=self.session_id, 
            host=urlparse(extras.get('extra__salesforce__instance_url')).hostname or None, 
            username=connection.login,
            password=connection.password,
            API_version=SalesforceBulk.DEFAULT_API_VERSION, 
            sandbox=extras.get('extra__salesforce__domain').lower().equal('test'),                
            security_token=extras.get('extra__salesforce__security_token') or None, 
            organizationId=extras.get('extra__salesforce__organization_id') or None, 
            client_id=extras.get('extra__salesforce__client_id') or None, 
            domain=extras.get('extra__salesforce__domain') or None
        )
        return conn

    def get_conn(self) -> SalesforceBulk:
        """Returns a Salesforce Bulk instance. (cached)"""
        return self.conn

    def _wait_for_batch(self, job, batch, timeout=60*10, sleep_interval=10):
        """
        Waits for a batch to complete. Logs progress to prevent zombies.

        :param job: The job to wait for.
        :type job: Job
        :param batch: The batch to wait for.
        :type batch: Batch
        :param timeout: The number of seconds to wait for the batch to complete.
        :type timeout: int
        :param sleep_interval: The number of seconds to sleep between polling the batch status.
        :type sleep_interval: int
        """
        waited = 0
        while not self.conn.is_batch_done(batch, job) and waited < timeout:
            log.debug('Sleeping between batch completion checks.  waited: %d' , waited)
            time.sleep(sleep_interval)
            waited += sleep_interval

    def execute_single_bulk_job(self, object_name, operation, contentType='CSV', concurrency=None, external_id_name=None, pk_chunking=False, soql=None, data_generator=None, **kwargs):
        """
        Executes a single Bulk Job

        :param object_name: The Bulk object to execute the job on
        :type object_name: str
        :param operation: The Bulk operation to perform
        :type operation: str
        :param contentType: The format in which the data will be provided to the Bulk API
        :type contentType: str
        :param concurrency: The number of concurrent connections to use per server
        :type concurrency: int
        :param external_id_name: The name of the external ID field used to match existing records for upserts
        :type external_id_name: str
        :param pk_chunking: Whether to use primary key chunking
        :type pk_chunking: bool
        :return: The Bulk Job results
        :rtype: List(UploadResult) or Iterable(IteratorBytesIO)
        """
        job = self.conn.create_job(object_name, operation, contentType, concurrency, external_id_name, pk_chunking)
        batch = None
        if operation == 'query' or operation == 'queryAll':
            batch = self.conn.query(job, soql, contentType)
        else:
            batch = self.conn.post_batch(job, data_generator)
        self.conn.close_job(job)
        self._wait_for_batch(job, batch, timeout=60*10, sleep_interval=10)
        if operation == 'query' or operation == 'queryAll':
            return self.conn.get_all_results_for_query_batch(batch, job)
        else:
            return self.conn.get_batch_results(batch, job)
