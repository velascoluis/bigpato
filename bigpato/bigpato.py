# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import duckdb
from sqlglot import parse_one, exp, transpile
from google.cloud import bigquery
from google.cloud import storage
import logging
import shutil
from collections import OrderedDict
import bigpato.common.constants as constants


class BigPato:
    def __init__(self,bq_project,bq_database,bq_key,duckdb_db,local_duck_folder,export_bq_bucket):
        self.__metadata_dict = {}
        self.__bq_project=bq_project
        #BQ dataset really - used database for name consistency
        self.__bq_database=bq_database
        self.__bq_key=bq_key
        self.__duckdb_db=duckdb_db
        self.__local_duck_folder=local_duck_folder
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=self.__bq_key
        self.__export_bq_bucket=export_bq_bucket
        #LRU Cache
        self.__lru_cache = OrderedDict()
        self.__lru_capacity = constants.LRU_TABLE_CAPACITY
        # Purge local storage
        self.__purge_local_storage()
        #Populate metadata
        self.__populate_metadata_bq()
        self.__populate_metadata_duckdb()
       
        
        
    
    def get_metadata_dict(self):
        return self.__metadata_dict

    def get_cache(self):
        return self.__lru_cache
 
    def launch_balance_storage(self):
        logging.info("Start rebalancing storage ..")
        for table in self.__lru_cache.keys():
            if self.__metadata_dict[table]['location'] != constants.LOCAL:
                self.__promote_table_to_local(table)

    def __purge_local_storage(self):
        if not os.path.exists('{}'.format(self.__local_duck_folder)):
            os.makedirs('{}'.format(self.__local_duck_folder))
      
    
    def __lru_get(self, table_name):
        if table_name not in self.__lru_cache:
            return -1
        else:
            self.__lru_cache.move_to_end(table_name)
            return self.cache[table_name]
    
    def __lru_put(self, table_name):
        self.__lru_cache[table_name] = table_name
        self.__lru_cache.move_to_end(table_name)
        if len(self.__lru_cache) > self.__lru_capacity:
            self.__lru_cache.popitem(last = False)
    
    
    def __check_all_tables_are_local(self,query):
        all_local = True
        query_duckdb = transpile(query, read=constants.BIGQUERY, write=constants.DUCKDB)[0]
        for table in parse_one(query_duckdb).find_all(exp.Table):
            if not table.name in self.__metadata_dict:
                raise ValueError("There is no table {} in dictionary".format(table))
            else:
                if self.__metadata_dict[table.name]['location'] != constants.LOCAL:
                    all_local = False
        return  all_local    
    
    
    def __populate_metadata_bq(self):
        logging.info("Populating metadata from BigQuery ..")
        client = bigquery.Client()
        tables = client.list_tables(self.__bq_database)  
        for table in tables:
            self.__metadata_dict[table.table_id] = {'location' : constants.BIGQUERY, 'usage': 0}
            
    def __populate_metadata_duckdb(self):
        logging.info("Populating metadata from duckDB ..")
        con = duckdb.connect(database=self.__duckdb_db, read_only=False)
        tables_registered = con.execute("SHOW TABLES").df()
        for table in [f.name for f in os.scandir(self.__local_duck_folder) if f.is_dir()]:
            self.__metadata_dict[table] = {'location' : constants.LOCAL, 'usage': 0}
            if table not in tables_registered['name'].values:
                #Add the table to the local duckdb dictionary
                con.execute("CREATE OR REPLACE TABLE {} AS SELECT * FROM read_parquet('{}/{}/*')".format(table,self.__local_duck_folder,table))
    

    def __exec_query_duckdb(self,query):
        logging.info("Executing query {} with duckDB ..".format(query))
        con = duckdb.connect(database=self.__duckdb_db, read_only=False)
        #Transpile to duckDB SQL dialect (from BQ)
        query_duckdb = transpile(query, read=constants.BIGQUERY, write=constants.DUCKDB)[0]
        output = con.execute(query_duckdb).df()
        self.__update_table_usage(query)
        return output
    
    def __exec_query_bq(self, query):
        logging.info("Executing query {} with BigQuery ..".format(query))
        client = bigquery.Client(project=self.__bq_project)
        # Append dataset to table name
        sql_tree = parse_one(query)
        for table in sql_tree.find_all(exp.Table):
            new_table = table.copy()
            new_table.set("this", "{}.{}".format(
                self.__bq_database, table.name))
            table.replace(new_table)
        query_job = client.query(sql_tree.sql())
        output = query_job.result().to_dataframe()
        self.__update_table_usage(query)
        return output
    
    
    def __update_table_usage(self,query):
        query_duckdb = transpile(query, read=constants.BIGQUERY, write=constants.DUCKDB)[0]
        for table in parse_one(query_duckdb).find_all(exp.Table):
            logging.info("Updating usage +1 for table {}".format(table.name))
            self.__metadata_dict[table.name]['usage'] = self.__metadata_dict[table.name]['usage'] + 1
            self.__lru_put(table.name)
    
    def __promote_table_to_local(self,table_name):
        logging.info("Table {} will be promoted to local storage".format(table_name))
        bq_client = bigquery.Client()
        storage_client = storage.Client()
        dataset_ref = bigquery.DatasetReference(self.__bq_project, self.__bq_database)
        table_ref = dataset_ref.table(table_name)
        export_gcs_filename = '{}/{}*'.format(table_name,table_name)
        job_config = bigquery.ExtractJobConfig()
        job_config.destination_format = bigquery.DestinationFormat.PARQUET
        job_config.print_header = False
        destination_uri = 'gs://{}/{}'.format(self.__export_bq_bucket, export_gcs_filename)
        extract_job = bq_client.extract_table(
            table_ref,
            destination_uri,
            job_config=job_config,
            location=constants.MULTI_REGION_LOCATION)  
        extract_job.result()
        logging.info(
            "Table {} extracted".format(table_name))
        #Create dir in local    
        if os.path.exists('{}/{}'.format(self.__local_duck_folder,table_name)):
            shutil.rmtree('{}/{}'.format(self.__local_duck_folder,table_name))
            os.makedirs('{}/{}'.format(self.__local_duck_folder,table_name))
        else:
            os.makedirs('{}/{}'.format(self.__local_duck_folder,table_name))
        #Download the parquet files
        command = "gsutil -m cp -r gs://{}/{} {}/{}/".format(self.__export_bq_bucket, export_gcs_filename,self.__local_duck_folder,table_name)
        os.system(command)
        logging.info(
            "Table {} downloaded".format(table_name))
        #Register the tables
        self.__populate_metadata_duckdb()
        
    def exec_query(self,query):
        if self.__check_all_tables_are_local(query):
            #All things equal duckDB is prioritized
            return self.__exec_query_duckdb(query)
        else:
            return self.__exec_query_bq(query)   