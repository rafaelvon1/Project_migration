from sqlalchemy import create_engine, select, Table, MetaData
import pandas as pd
from connect import Connection
class Postgre(Connection):
    def __init__(self,user, password, host, port, database):
        self.DB_USER_PTG = user
        self.DB_PWD_PTG = password
        self.DB_HOST_PTG = host
        self.DB_PORT_PTG = port
        self.DB_DB_PTG = database
        self.metadata = MetaData()
    def Engine(self):
        try:
            conn_str = (
                f"postgresql+psycopg2://"
                f"{self.DB_USER_PTG}:{self.DB_PWD_PTG}"
                f"@{self.DB_HOST_PTG}:{self.DB_PORT_PTG}"
                f"/{self.DB_DB_PTG}"
            )

            engine = create_engine(conn_str)
            return engine

        except Exception as e:
            raise Exception(f"Erro ao criar engine: {e}")
    def Query_all(self,conn,table,schema) -> pd.DataFrame:
        try:
            table_obj = Table(table, self.metadata, autoload_with=self.Engine(), schema=schema)

            stmt = select(table_obj)
            
            result = conn.execute(stmt)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            return df
        except Exception as e:
            raise e