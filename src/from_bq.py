"""
Contains data loader for reading data from BigQuery into Streamlit application
"""
from typing import Callable, List, Optional
from concurrent.futures import ProcessPoolExecutor, wait

from streamlit import cache, secrets
from pandas import DataFrame, concat
from google.oauth2 import service_account
from google.cloud import bigquery_storage, bigquery
from google.cloud.bigquery_storage_v1beta2 import types as bq_types

from datatypes import BQResourceType

class PandasBQLoader:
    """
    Handles loading data from BigQuery client to a dataframe with Apache Arrow
    """
    project_name = "sandbox-293422"
    dataset_id = "edu_az"
    credentials = service_account.Credentials.from_service_account_info(
        secrets["gcp_service_account"]
    )
    client = bigquery_storage.BigQueryReadClient(credentials=credentials)
    query_client = bigquery.Client(credentials=credentials)

    @property
    def table(self) -> str:
        """ Resource definition of instance table_id """
        return self._resource_uri()

    @property
    def project(self) -> str:
        """ Resource definition of the loaders' project """
        return self._resource_uri("project")

    @property
    def dataset(self) -> str:
        """ Resource definition of the project dataset """
        return self._resource_uri("dataset")

    def __init__(self, *,
        table_id: str,
        row_restriction: Optional[str] = None,
        fields: Optional[List[str]] = None,
    ):
        """
        Initialize a dataset loader for a specific table with name
        :param: `table_id`, and restrictions for reading from certain columns
        :param: `fields` and rows
        :param: `row_restriction`
        """
        self.table_id = table_id

        options = {}
        if fields:
            options['selected_fields'] = fields
        if row_restriction:
            options['row_strection'] = row_restriction
        self.read_options = bq_types.ReadSession.TableReadOptions(**options)

        self.req_session = bq_types.ReadSession(
            table=self.table,
            data_format=bq_types.DataFormat.ARROW,
            read_options=self.read_options
        )

    @cache(ttl=600)
    def stream(self, stream_count: int = 1) -> DataFrame:
        """ Run parallel stream loader of  """
        read_session = PandasBQLoader.client.create_read_session(
            parent=self.project,
            read_session=self.req_session,
            max_stream_count=stream_count,
        )

        return PandasBQLoader._parallelize(
            PandasBQLoader._stream,
            read_session,
            stream_count
        )

    @cache(ttl=600)
    def query(self, query: str) -> DataFrame:
        """ Execute special query on BigQuery dataset """
        return (
            self.query_client
                .query(query)
                .result()
                .to_dataframe()
        )

    @staticmethod
    def _parallelize(
        func: Callable[[int], DataFrame],
        session: bq_types.ReadSession,
        stream_count: int = 1
    ) -> DataFrame:
        """ Use concurrent.futures to parallelize stream task """
        with ProcessPoolExecutor() as exc:
            futures = [
                exc.submit(func, session, idx) for idx in range(stream_count)
            ]
            dfs = [
                future.result()
                for future in wait(futures, return_when="ALL_COMPLETED").done
            ]
        return concat(dfs)

    @staticmethod
    def _stream(
        session: bq_types.ReadSession,
        stream_idx: int = 0
    ) -> DataFrame:
        """ Iterates over session stream rows and creates a giant dataframe """
        pages = PandasBQLoader.client.read_rows(
            session.streams[stream_idx].name
        ).rows().pages
        return concat([message.to_dataframe() for message in pages])

    def _resource_uri(self, rtype: BQResourceType = "table") -> str:
        """ Creates table endpoint string from template """
        project_uri = f"projects/{PandasBQLoader.project_name}"
        if rtype == "project":
            return project_uri

        dataset_uri = f"{project_uri}/datasets/{PandasBQLoader.dataset_id}"
        if rtype == "dataset":
            return dataset_uri

        table_uri = f"{dataset_uri}/tables/{self.table_id}"
        return table_uri
