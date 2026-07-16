import pandas as pd
class UpSert:
    def __init__(self):
        pass

    def insert(self,df_src,df_dest):
        return (
            df_src.merge(
                df_dest[["id"]],
                on="id",
                how="left",
                indicator=True
            )
            .loc[lambda df: df["_merge"] == "left_only", df_src.columns]
            .reset_index(drop=True)
        )
        
    def update(self,df_src,df_dest):
        return(
            df_src.merge(
                df_dest[["id", "hash_id"]],
                on="id",
                how="inner",
                suffixes=("", "_dest")
            )
            .dropna(subset=["hash_id_dest"])
            .loc[
                lambda df: df["hash_id"] != df["hash_id_dest"], df_src.columns
            ]
            .reset_index(drop=True)
        )
    def delete(self,df_src,df_dest) -> pd.DataFrame:
        return (
            df_dest.merge(
                df_src[["id"]],
                on="id",
                how="left",
                indicator=True
            )
            .loc[lambda df: df["_merge"] == "left_only", df_dest.columns]
            .reset_index(drop=True)
        )