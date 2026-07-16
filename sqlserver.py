from sqlalchemy import create_engine, select, Table, MetaData, delete
import pandas as pd
import urllib
from connect import Connection
from logger import logger

class SqlServer(Connection):
    def __init__(self,driver, host, database, user, password):
        self.DB_DRIVER_SQL = driver
        self.DB_HOST_SQL = host
        self.DB_NAME_SQL = database
        self.DB_USER_SQL = user
        self.DB_PWD_SQL = password
        self.metadata = MetaData()
    def Engine(self):
        try:
            # string ODBC padrao
            conn_str = (
                f"DRIVER={{{self.DB_DRIVER_SQL}}};"
                f"SERVER={self.DB_HOST_SQL};"
                f"DATABASE={self.DB_NAME_SQL};"
                f"UID={self.DB_USER_SQL};"
                f"PWD={self.DB_PWD_SQL};"
                "TrustServerCertificate=yes;"
            )
            # escapa para uso na URL
            params = urllib.parse.quote_plus(conn_str)
            engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
            return engine
        except Exception as e:
            raise Exception(f"Erro ao criar engine: {e}")
    def Query_all(self,conn,table,schema) -> pd.DataFrame:
        table_obj = Table(table, self.metadata, autoload_with=self.Engine(), schema=schema)
        try:
            table_obj = Table(table, self.metadata, autoload_with=self.Engine(), schema=schema)

            stmt = select(table_obj)
            
            result = conn.execute(stmt)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            return df
        except Exception as e:
            raise e
    def Query_Delete_all(self, conn, table, schema="dbo"):
        try:
            # 1. Reflete a tabela (SQLAlchemy Core)
            table_obj = Table(
                table,
                self.metadata,
                autoload_with=self.Engine(),
                schema=schema
            )

            # 2. Testa existência / dados (pega 1 linha)
            test_stmt = select(1).select_from(table_obj).limit(1)
            result = conn.execute(test_stmt).fetchone()

            if result is None:
                msg = f"Tabela {schema}.{table} não existe ou está vazia."
                logger.info(msg)
                return msg

            # 3. DELETE usando SQLAlchemy Core (sem SQL manual)
            delete_stmt = delete(table_obj)
            conn.execute(delete_stmt)

            msg = f"Tabela {schema}.{table} deletada com sucesso."
            logger.info(msg)
            return msg

        except Exception as e:
            logger.error(f"Erro ao deletar {schema}.{table}: {e}")
            raise
    def Save(self, df, conn, table, schema="dbo"):
        try:
            df.to_sql(
                        name=table,
                        schema=schema,
                        con=conn,
                        if_exists="append",
                        index=False
                    )
        except Exception as e:
            logger.error(f"Erro ao salvar dados na tabela {schema}.{table}: {e}")
            raise
    def Update_where(self,pk,df_update, conn, table, schema="dbo"):
        table_obj = Table(
                table,
                self.metadata,
                autoload_with=self.Engine(),
                schema=schema
            )
        for _, row in df_update.iterrows():
            dados = row.to_dict()
            id_registro = dados.pop(pk)  # remove o id do dicionário

            stmt = (
                table_obj.update()
                .where(table_obj.c[pk] == id_registro)
                .values(**dados)
            )

            conn.execute(stmt)
    def Delete_where(self,pk, id_delete,conn, table, schema="dbo"):
        table_obj = Table(
                table,
                self.metadata,
                autoload_with=self.Engine(),
                schema=schema
            )
       
        try:
            stmt = delete(table_obj).where(table_obj.c[pk].in_(id_delete))
            conn.execute(stmt)
            print(f"Registros com ID {id_delete} deletados da tabela {schema}.{table} com sucesso.")
        except Exception as e:
            logger.error(f"Erro ao deletar dados na tabela {schema}.{table}: {e}")
            raise "Algo deu errado: " + str(e)