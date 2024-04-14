from include.databricks_config import server_hostname, http_path, access_token
# from databricks_config import server_hostname, http_path, access_token
from databricks import sql

class databricksconnect():
    def __init__(self) -> None:
        self.hostname = server_hostname
        self.httppath = http_path
        self.api_token = access_token
    def buildconn(self):
        self.connection = sql.connect(server_hostname = self.hostname,
                                      http_path = self.httppath,
                                      access_token = self.api_token)
        return self.connection.cursor()
    def closeconn(self):
        self.connection.close()

if __name__ == "__main__":
    db_connect = databricksconnect()
    print("Connecting with Databricks...")
    db_connect.buildconn()
    print("Connected!")
    db_connect.closeconn()
    print("Close the Connection...")