from sqlalchemy import engine
import requests as rq
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
def get_api(url) -> pd.DataFrame:
    try:
        response = rq.get(url).json()
        df = pd.DataFrame(response['products'])
        return df
    except Exception as e:
        raise e

if __name__ == "__main__":
    logger.info("iniciando o processo de ETL")
    DATA = json_data()
    # -----------------------
    # Parametros de DB
    # -----------------------
    DB_USER_PTG = DATA["connection_postgre"]["user"]
    DB_PWD_PTG = DATA["connection_postgre"]["password"]
    DB_HOST_PTG = DATA["connection_postgre"]["host"]
    DB_PORT_PTG = DATA["connection_postgre"]["port"]
    DB_DB_PTG = DATA["connection_postgre"]["database"]
    table_name = "products"
    df = 0
    column_md5 = "hash_id"
    conn_ptg = Postgre(DB_USER_PTG, DB_PWD_PTG, DB_HOST_PTG, DB_PORT_PTG, DB_DB_PTG)

    engine_ptg = conn_ptg.Engine()


    df = get_api("https://dummyjson.com/products")
    df = df[["id", "description", "title", "price", "category", "images"]]
    print(df.columns)
    df.to_csv(f"dados_csv/{table_name}.csv", index=False)
    df = HashMD5().create_columns_md5(df, column_md5)
    try:
        with engine_ptg.begin() as conn:
            
            df.to_sql(
                name=table_name,
                con=conn,
                if_exists="append",
                index=False
            )
            print("dados inseridos com sucesso")
    except Exception as e:
        raise e
         
    


