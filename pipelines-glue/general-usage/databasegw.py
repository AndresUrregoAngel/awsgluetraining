import awswrangler as wr
from pgdb import connect
from errors import ReadingCatalogTables

class athenagw:
    def __init__(self,args):
        self.table = "tbl_shooting"
        self.catalog = args['catalog']
        
        
    def callingtablescontent(self):
        try:
            df =  wr.athena.read_sql_query("SELECT * FROM tbl_shooting", database=self.catalog)
            
            return df 
            
        except Exception as error:
            raise ReadingCatalogTables(error)
            
            
    def storeredshift(self,df):
        try:
            
            eng_redshift = wr.catalog.get_engine("redshiftwh-pipelines")
            wr.db.to_sql(df, eng_redshift, schema="public", name="tbl_python_shooting", if_exists="replace", index=False)  # Redshift
            
        except Exception as error:
            raise ReadingCatalogTables(error)
            
    
    def storerds(self,df):
        try:
            
            eng_redshift = wr.catalog.get_engine("jdbcrds")
            wr.db.to_sql(df, eng_redshift, name="tbl_python_shooting", if_exists="replace", index=False)  
            
        except Exception as error:
            raise ReadingCatalogTables(error)
        