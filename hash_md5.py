import pandas as pd 
import hashlib
class HashMD5:
    def __init__(self):
        pass

    def generate_md5(self, row):
        texto = '|'.join(map(str,row.values))
        return hashlib.md5(texto.encode()).hexdigest()
        
    def create_columns_md5(self,df:pd.DataFrame, column_name: str) -> pd.DataFrame:
        df_md5 = df.copy()
        df_md5[column_name] = df.apply(self.generate_md5, axis=1)
        return df_md5