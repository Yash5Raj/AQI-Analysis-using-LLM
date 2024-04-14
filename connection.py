from streamlit.connections import ExperimentalBaseConnection
from streamlit.runtime.caching import cache_data
import databricks
import pandas as pd

class DatabricksConnection(ExperimentalBaseConnection[databricks]):
    def _connect(self, **kwargs):
        # setting serverhost name
        if 'server_hostname' in kwargs:
            server_hostname = kwargs.pop('server_hostname')
        else:
            server_hostname = self._secrets['server_hostname']
        
        # setting http_path
        if 'http_path' in kwargs:
            http_path = kwargs.pop('http_path')
        else:
            http_path = self._secrets['http_path']
        
        # setting access_token
        if 'access_token' in kwargs:
            access_token = kwargs.pop('access_token')
        else:
            access_token = self._secrets['access_token']
              
        return databricks.sql.connect(http_path=http_path, server_hostname=server_hostname,access_token=access_token, **kwargs)

    def cursor(self):
        return self._instance.cursor()

    def query(self, query: str, ttl: int = 3600, **kwargs) -> pd.DataFrame:
        @cache_data(ttl=ttl)
        def _query(query: str, **kwargs) -> pd.DataFrame:
            cursor = self.cursor()
            cursor.execute(query, **kwargs)
            result = cursor.fetchall()
            return pd.DataFrame([result[x].asDict() for x in range(len(result))])
        
        return _query(query, **kwargs)