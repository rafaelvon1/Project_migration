from sqlalchemy import engine

from logger import logger
import json
from sqlserver import SqlServer
from postgre import Postgre
from hash_md5 import HashMD5
#from compare_columns import compare_columns
import pandas as pd

def json_data():
    with open("config.json", "r") as f:
        return json.load(f)



if __name__ == "__main__":
    logger.info("iniciando o processo de ETL")
    DATA = json_data()
    # -----------------------
    # Parametros de DB
    # -----------------------
    DB_DRIVER_SQL = DATA["connection_sqlserver"]["driver"]
    DB_HOST_SQL = DATA["connection_sqlserver"]["server"]
    DB_NAME_SQL = DATA["connection_sqlserver"]["database"]
    DB_USER_SQL = DATA["connection_sqlserver"]["username"]
    DB_PWD_SQL = DATA["connection_sqlserver"]["password"]

    DB_USER_PTG = DATA["connection_postgre"]["user"]
    DB_PWD_PTG = DATA["connection_postgre"]["password"]
    DB_HOST_PTG = DATA["connection_postgre"]["host"]
    DB_PORT_PTG = DATA["connection_postgre"]["port"]
    DB_DB_PTG = DATA["connection_postgre"]["database"]

    
    conn_sql = SqlServer(DB_DRIVER_SQL, DB_HOST_SQL, DB_NAME_SQL, DB_USER_SQL, DB_PWD_SQL)
    conn_ptg = Postgre(DB_USER_PTG, DB_PWD_PTG, DB_HOST_PTG, DB_PORT_PTG, DB_DB_PTG)

    engine_ptg = conn_ptg.Engine()
    engine_sql = conn_sql.Engine()
    for table in DATA["source_tables"]:
        try:
            table_name_origem = DATA["source_tables"][table]["table_origem"]
            table_name_destino = DATA["source_tables"][table]["table_destino"]
            schema_origem = DATA["source_tables"][table]["schema_origem"]
            schema_destino = DATA["source_tables"][table]["schema_destino"]
            column_md5 = DATA["source_tables"][table]["column_MD5"]
            logger.info(f"dados Json Carregados")
            print(f"dados Json Carregados")
        except Exception as e:
            logger.error(f"Erro ao consultar dados Json: {e}")
            print(f"Erro ao consultar dados Json: {e}")


        try:
            with engine_ptg.connect() as conn:
                data_postgre = conn_ptg.Query_all(conn, table_name_origem, schema_origem)
                df = pd.DataFrame(data_postgre)
            df.to_csv(f"dados_csv/{table_name_destino}.csv", index=False)
            print(f"Dados da tabela {table_name_destino} exportados")
            logger.info(f"Dados da tabela {table_name_destino} exportados ")
        except Exception as e:
            logger.error(f"Erro ao consultar dados da tabela {table_name_destino} no PostgreSQL: {e}")
            print(f"Erro ao consultar dados da tabela {table_name_destino} no PostgreSQL: {e}")

        try:
            df = HashMD5().create_columns_md5(df, column_md5)
            logger.info(f"Coluna MD5 gerada para a tabela {table_name_destino}")
            print(f"Coluna MD5 gerada para a tabela {table_name_destino}")

        except Exception as e:
            logger.error(f"Erro ao gerar coluna MD5 para a tabela {table_name_destino}: {e}")
            print(f"Erro ao gerar coluna MD5 para a tabela {table_name_destino}: {e}")
        try:

            with engine_sql.begin() as conn:

                conn_sql.Query_Delete(conn, table_name_destino)
                conn_sql.Save(df, conn, table_name_destino, schema_destino)
                
                logger.info(f"Dados da tabela {table_name_destino} migrados com sucesso para o SQL Server")
                print(f"Dados da tabela {table_name_destino} migrados com sucesso para o SQL Server")
        except Exception as e:
            logger.error(f"Erro ao inserir dados na tabela {table_name_destino} no SQL Server: {e}")
            print(f"Erro ao inserir dados na tabela {table_name_destino} no SQL Server: {e}")
         
    


