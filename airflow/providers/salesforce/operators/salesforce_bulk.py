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
from typing import Any
from airflow.models import BaseOperator
from airflow.providers.salesforce.hooks.salesforce_bulk import SalesforceBulkHook


class SalesforceBulkOperator(BaseOperator):
    """
    Executes the Salesforce Bulk API

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SalesforceBulkOperator`

    :param endpoint: The REST endpoint for the request.
    :type endpoint: str
    :param method: HTTP method for the request (default GET)
    :type method: str
    :param payload: A dict of parameters to send in a POST / PUT request
    :type payload: str
    :param salesforce_conn_id: The :ref:`Salesforce Connection id <howto/connection:SalesforceHook>`. Only Password and Direct Session are supported for Bulk API.
    :type salesforce_conn_id: str
    """

    def __init__(
        self,
        *,
        object_name: str, 
        operation: str, 
        contentType: str = "CSV", 
        concurrency: int = None, 
        external_id_name: str = None, 
        pk_chunking: Any = False, 
        soql: str = None, 
        data_generator: Any = None,
        salesforce_conn_id: str = 'salesforce_default',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.object_name = object_name
        self.operation = operation
        self.contentType = contentType
        self.concurrency = concurrency
        self.external_id_name = external_id_name
        self.pk_chunking = pk_chunking
        self.soql = soql
        self.data_generator = data_generator
        self.salesforce_conn_id = salesforce_conn_id

    def execute(self, context: dict) -> dict:
        """
        Creates a Salesforce Bulk API job, waits for it to complete, and the pushes results to xcom.
        :param context: The task context during execution.
        :type context: dict
        :return: BulkAPI response
        :rtype: dict
        """
        result: dict = {}
        sf_hook = SalesforceBulkHook(salesforce_conn_id=self.salesforce_conn_id)
        conn = sf_hook.get_conn()
        execution_result = conn.execute_single_bulk_job(self.object_name, self.operation, self.contentType, self.concurrency, self.external_id_name, self.pk_chunking, self.soql, self.data_generator)
        
        if self.do_xcom_push:
            result = execution_result

        return result
