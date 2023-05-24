
#Load Data in to TeraData
from teradataml.dataframe.dataframe import in_schema
##Slow Load
from teradataml.dataframe.copy_to import copy_to_sql
##FAST LOAD
from teradataml.dataframe.fastload import fastload
from teradatasqlalchemy.types import *
#Make a Connection
from teradataml.context.context import *
from teradataml import create_context,DataFrame,db_list_tables


import pandas as pd

class TeraDataConnection():
    def __init__(self,username,password,host,logmech,database = None):
        self.username = username
        self.password = password
        self.host=host
        self.logmech = logmech
        self.database = database
        self.connect()
        
    def __enter__(self):
        """
        Creates the Teradata context and establishes the connection.
        """
        create_context(
            username=self.username,
            password=self.password,
            host=self.host,
            logmech=self.logmech
        )
        self.connection = get_connection()
        return self.connection
    
    def __exit__(self, exc_type, exc_value, traceback):
        """
        Cleans up the connection.
        """
        if self.connection is not None:
            self.connection.close()
    
    def execute_query(self,query):
        self.connection.execute(query)

    def fetch_query(self,query):
        df_list = []
        df_temp = self.connection.execute(query)
        for a in df_temp:
            df_list.append(a)
        df_temp = pd.DataFrame(df_list)

        return(df_temp)
    
    def copy_to_table(self, dataframe, table_name,schema_name,p_idx, C_S = 16383,types_x = None):
        if ((types_x != None) and (type(types_x) == dict)):
            copy_to_sql(dataframe
                        ,table_name
                        , schema_name
                        ,if_exists="replace"
                        ,primary_index=p_idx
                        ,chunksize = C_S,
                        types = types_x)
        else:
            copy_to_sql(dataframe
            ,table_name
            , schema_name
            ,if_exists="replace"
            ,primary_index=p_idx
            ,chunksize = C_S)

    def fast_load_to_table(self, dataframe, table_name,schema_name,p_idx):
        fastload(dataframe,table_name
                 ,schema_name = schema_name
                 ,primary_index=p_idx
                 ,if_exists="replace"
                )
    
    def drop_table(self, table_name):
        query = f'DROP TABLE {table_name}'
        return(self.execute_sql(query))

    def close_connection(self):
        """
        Closes the Teradata connection and removes the context.
        """
        if self.connection:
            self.connection.close()
        remove_context()

    def create_database(self, database_name):
        """
        Creates a new database in Teradata.

        Args:
            database_name (str): The name of the database to create.
        """
        query = f"CREATE DATABASE {database_name}"
        self.execute_query(query)

    def get_database_size(self, database_name):
        """
        Retrieves the size of a database in Teradata.

        Args:
            database_name (str): The name of the database.

        Returns:
            int: The size of the database in bytes.
        """
        query = f"SHOW SPACE DATABASE {database_name}"
        result = self.fetch_query(query)
        size = result['CurrentPerm'].values[0]
        return size

    def copy_table(self, source_table, target_table):
        """
        Copies the data from a source table to a target table in Teradata.

        Args:
            source_table (str): The name of the source table.
            target_table (str): The name of the target table.
        """
        query = f"INSERT INTO {target_table} SELECT * FROM {source_table}"
        self.execute_query(query)

    def rename_table(self, old_table_name, new_table_name):
        """
        Renames a table in Teradata.

        Args:
            old_table_name (str): The current name of the table.
            new_table_name (str): The new name for the table.
        """
        query = f"RENAME TABLE {old_table_name} TO {new_table_name}"
        self.execute_query(query)
    
    def create_view(self, view_name, query):
        """
        Creates a view in Teradata.

        Args:
            view_name (str): The name of the view to create.
            query (str): The SQL query defining the view.
        """
        query = f"CREATE VIEW {view_name} AS {query}"
        self.execute_query(query)

    def get_table_columns(self, table_name):
        """
        Retrieves the column names of a table in Teradata.

        Args:
            table_name (str): The name of the table.

        Returns:
            List[str]: A list of column names.
        """
        query = f"SELECT * FROM {table_name} WHERE 1=0"
        result = self.fetch_query(query)
        columns = result.columns.tolist()
        return columns

    def execute_multiple_queries(self, queries):
        """
        Executes multiple SQL queries in a single call to the Teradata database.

        Args:
            queries (List[str]): A list of SQL queries to execute.
        """
        for query in queries:
            self.execute_query(query)

    def join_tables(self, table1, table2, join_condition, columns=None):
        """
        Performs a join operation between two tables in Teradata.

        Args:
            table1 (str): The name of the first table.
            table2 (str): The name of the second table.
            join_condition (str): The join condition to be used.
            columns (List[str], optional): The columns to select from the joined tables.
                If not specified, all columns are selected.

        Returns:
            pd.DataFrame: The result of the join operation as a Pandas DataFrame.
        """
        if columns is None:
            columns = "*"
        else:
            columns = ", ".join(columns)

        query = f"SELECT {columns} FROM {table1} JOIN {table2} ON {join_condition}"
        result = self.fetch_query(query)
        return result

    def execute_transaction(self, queries):
        """
        Executes a transaction in Teradata.

        Args:
            queries (List[str]): A list of SQL queries to execute as part of the transaction.
        """
        self.execute_query("BEGIN TRANSACTION;")
        try:
            for query in queries:
                self.execute_query(query)
            self.execute_query("COMMIT;")
        except:
            self.execute_query("ROLLBACK;")
            raise

    def get_table_statistics(self, table_name):
        """
        Retrieves statistics for a table in Teradata.

        Args:
            table_name (str): The name of the table.

        Returns:
            pd.DataFrame: The table statistics as a Pandas DataFrame.
        """
        query = f"HELP STATS {table_name}"
        result = self.fetch_query(query)
        return result
    
    def execute_stored_procedure(self, procedure_name, parameters=None):
        """
        Executes a stored procedure in Teradata.

        Args:
            procedure_name (str): The name of the stored procedure.
            parameters (dict, optional): A dictionary of input parameters and their values.

        Returns:
            pd.DataFrame: The result of the stored procedure as a Pandas DataFrame.
        """
        if parameters is None:
            parameters = {}

        param_values = ", ".join([f"{k}={v}" for k, v in parameters.items()])
        query = f"CALL {procedure_name}({param_values})"
        result = self.fetch_query(query)
        return result
    
    def run_teradata_sql_file(self, file_path):
        """
        Runs a Teradata SQL file containing multiple SQL statements.

        Args:
            file_path (str): The path to the SQL file.
        """
        with open(file_path, "r") as file:
            sql_statements = file.read().split(";")

        for statement in sql_statements:
            statement = statement.strip()
            if statement:
                self.execute_query(statement)

    def get_table_schema(self, table_name):
        """
        Retrieves the schema of a table in Teradata.

        Args:
            table_name (str): The name of the table.

        Returns:
            pd.DataFrame: The table schema as a Pandas DataFrame.
        """
        query = f"SHOW TABLE {table_name}"
        result = self.fetch_query(query)
        return result

    def export_table_to_csv(self, table_name, csv_file_path):
        """
        Exports a table from Teradata to a CSV file.

        Args:
            table_name (str): The name of the table.
            csv_file_path (str): The path to the output CSV file.
        """
        query = f"EXPORT DATA TABLE {table_name} INTO FILE '{csv_file_path}'"
        self.execute_query(query)

    def import_csv_to_table(self, csv_file_path, table_name, delimiter=",", skip_rows=1):
        """
        Imports data from a CSV file to a Teradata table.

        Args:
            csv_file_path (str): The path to the CSV file.
            table_name (str): The name of the target table.
            delimiter (str, optional): The delimiter used in the CSV file.
            skip_rows (int, optional): The number of rows to skip from the beginning of the CSV file.
        """
        query = f"COPY TABLE {table_name} FROM '{csv_file_path}' USING DELIMITED BY '{delimiter}' SKIP {skip_rows}"
        self.execute_query(query)

    def export_table_to_csv(self, table_name, file_path):
        """
        Exports the data from a Teradata table to a CSV file.

        Args:
            table_name (str): The name of the table to export.
            file_path (str): The file path for the CSV output.
        """
        query = f"SELECT * FROM {table_name}"
        result = self.fetch_query(query)
        result.to_csv(file_path, index=False)


class TeradataDataPrep(TeraDataConnection):
    def __init__(self,username,password,host,logmech,database = None):
        super().__init__(username,password,host,logmech)
    
    #Clean Data 
    #Transform Data
    #AGG DATA
    #Feature Engineering
    

class TeradataDataExplore(TeraDataConnection):
    def __init__(self,username,password,host,logmech,database = None):
        super().__init__(username,password,host,logmech)

    def get_dbs(self,Database_Name):
        #GET CURRENT USER U_ID
        df_temp = self.fetch_query('SELECT Current_User')
        u_id = df_temp['Current_User'].tolist()[0]
        #FETCH DBs User Has ACCESS TO
        sql = f'''
            SELECT DISTINCT DatabaseName
            FROM dbc.rolemembers A
            LEFT JOIN DBC.ALLROLERIGHTS B
                ON A.ROLENAME = B.ROLENAME
            WHERE grantee = '{u_id}' 
        '''
        #QUERY
        df_dbs = self.Query_2_Pandas(sql)
        #RETURN LIST
        df_dbs = df_dbs['DatabaseName'].str.strip().tolist()
        return(df_dbs)

    def get_tables(self,Database_Name):
        db_list = db_list_tables(Database_Name)
        db_list = db_list['TableName'].tolist()
        return(db_list)

    def TD_DF_MetaData(self,TD_DF,method = '', property = ''):
        if method:
            if method.lower() == 'info':
                TD_DF.info()
            if method.lower() == 'keys':
                TD_DF.keys()
        if property:
            if property.lower() == 'columns':
                meta_data = TD_DF.columns
            if property.lower() == 'dtypes':
                meta_data = TD_DF.dtypes
            if property.lower() == 'index':
                meta_data = TD_DF.index
            if property.lower() == 'shape':
                meta_data = TD_DF.shape
            if property.lower() == 'size':
                meta_data = TD_DF.size
        return(meta_data)
    
    def TD_DF_FROM_QUERY(self,query):
        df = DataFrame.from_query(query)
        return(df)

    def TD_DF_FROM_TABLE(self,TABLE_NAME):
        df = DataFrame.from_query(TABLE_NAME)
        return(df)

    def TD_DF_FROM_DBC(self,DBC_TABLE_NAME):
        df = DataFrame(in_schema("dbc",DBC_TABLE_NAME))
        return(df)

    def TD_DF_MetaData(self,TD_DF,method = '', property = ''):
        if method:
            if method.lower() == 'info':
                TD_DF.info()
            if method.lower() == 'keys':
                TD_DF.keys()
        if property:
            if property.lower() == 'columns':
                meta_data = TD_DF.columns
            if property.lower() == 'dtypes':
                meta_data = TD_DF.dtypes
            if property.lower() == 'index':
                meta_data = TD_DF.index
            if property.lower() == 'shape':
                meta_data = TD_DF.shape
            if property.lower() == 'size':
                meta_data = TD_DF.size
        return(meta_data)
    
    def TD_DF_FROM_QUERY_2_Pandas(self,query, all_rows_ = False, fastexport_ = False):
        df = DataFrame.from_query(query)
        pdf_df = df.to_pandas(all_rows = all_rows_, fastexport = fastexport_)
        return(pdf_df)
    
    def TD_DF_2_PANDAS(self,TD_DF, all_rows_ = False, fastexport_ = False):
        df = TD_DF.to_pandas(all_rows = all_rows_, fastexport = fastexport_)
        return(df)

    def TD_DF_2_CSV(self,TD_DF,csv_name = 'Test_DF.csv', all_rows_ = False, fastexport_ = False):
        df = TD_DF.to_csv(csv_name,all_rows = all_rows_, fastexport = fastexport_)
        return(df)

    def Query_2_Pandas(self,query,all_rows_ = False, fastexport_ = False):
        df = self.TD_DF_FROM_QUERY(query)
        df = self.TD_DF_2_PANDAS(df,all_rows_, fastexport_)
        return(df)

    def QUERY_2_CSV(self,query,csv_name = 'Test_DF.csv', all_rows_ = False, fastexport_ = False):
        df = self.TD_DF_FROM_QUERY(query)
        self.to_csv(df,csv_name,all_rows = all_rows_, fastexport = fastexport_)
    
    #Data Profiling
    def get_table_row_count(self, table_name):
        query = f'SELECT COUNT(*) FROM {table_name}'
        session_df = self.execute_sql(query)
        value = session_df.values[0][0]
        return(value)
    #Outlier Detection

    

class TeradataDataViz(TeraDataConnection):
    def __init__(self,username,password,host,logmech,database = None):
        super().__init__(username,password,host,logmech)

    #Create Chart 

    #Create Report
    pass
