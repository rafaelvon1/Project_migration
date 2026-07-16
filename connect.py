from sqlalchemy import create_engine, select, Table, MetaData
import pandas as pd

class Connection:
    def __init__(self):
        self.metadata = MetaData()
    
    def Engine(self):
        raise NotImplementedError(
            "A classe filha deve implementar o método engine()"
        )

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
    def Query_Delete_all(self):
        raise NotImplementedError(
            "A classe filha deve implementar o método save()"
        )
    def Save(self):
        raise NotImplementedError(
            "A classe filha deve implementar o método save()"
        )
    def Update_where(self):
        raise NotImplementedError(
            "A classe filha deve implementar o método update_where()"
        )
    def Delete_where(self):
        raise NotImplementedError(
            "A classe filha deve implementar o método delete_where()"
        )
