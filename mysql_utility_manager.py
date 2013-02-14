'''
Created on Jan 25, 2013

@author: sherman
'''
import mysql_conn_manager

class utility_manager(object):
    '''
    Description: Wraps the mysql connection manager with some commonly used utility methods
    '''

    __db_manager=None
    def __init__(self,databases):
        '''
        instantiates the connection manager object
        '''
        self.__db_manager=mysql_conn_manager.mysql_query_manager(databases)
        
    '''
    Input: server name, database name
    Output: query would be executed on the specified database under the provided database server name
    '''
    def execute_query_on_db(self,query,sv_name,db_name,stop_on_error=False):
        use_statement="use "+db_name+";"
        self.__db_manager.execute_query(use_statement, sv_name)
        rows=self.__db_manager.execute_query(query, sv_name, stop_on_error)
        return rows
    
    '''
    Input: database server name, list of databases to drop from server
    Output: drops databases from server
    '''        
    def drop_databases(self,sv_name,droplist,repeat_on_error=False):        
            base_query="drop database "
            for dbname in droplist:
                query=base_query+dbname+";"
                self.__db_manager.execute_query(query,sv_name,repeat_on_error)
                #self.execute_query("commit", sv_name,repeat_on_error)
    '''
    Input: database server name,database name,tables list to drop
    Output: drops tables in the provided list from the provided database on the provided server name
    '''
    def drop_tables(self,sv_name,db_name,tables):
        for table in tables:
            query='drop table '+table+';'
            self.execute_query_on_db(query, sv_name, db_name)
            
    '''
    checks if table exist in db
    Input: database server name, database name, table name
    Output: returns true if table in db, else return false
    '''
    def check_table_exist(self,sv_name,db_name,table):
        use_statement="use "+db_name+";"
        self.__db_manager.execute_query(use_statement, sv_name)
        query="show tables;"
        tables_dict=self.__db_manager.execute_query(query, sv_name)
        tables=[]
        for table_dict in tables_dict:
            tables.append(table_dict.values()[0])   
        if table in tables:
            return True
        else:
            return False
    

    
    '''
    Input: server name, excluded databases
    Output: return the list of databases for the provided instance, excluding the databases from the list provided
    '''    
    def get_databases(self,sv_name,excluded_databases=[]):
        query="show databases;"
        rows=self.__db_manager.execute_query(query, sv_name)
        databases=[]
        for row in rows:
            if not (row['Database'] in excluded_databases):
                databases.append(row['Database'])
        return databases         

    '''
    Input: server name, database name
    Output: return the list of tables for the provided database on the server
    '''    
    def get_tables(self,sv_name,db_name):
        query="show tables;"
        rows=self.execute_query_on_db(query, sv_name, db_name)
        tables=[]
        for row in rows:
            tables.append(row.values()[0])
        return tables
    
    '''
    Input: server name, database name
    Output: returns true if table is truncated, else return false
    '''
    def is_truncated(self,sv_name,db_name,table):
        query="select * from "+table+" LIMIT 1;"
        rows=self.execute_query_on_db(query, sv_name, db_name)
        if rows==0:
            return True
        else:
            return False         

    def truncate_tables(self,sv_name,db_name,tables):
        for table in tables:
            query="truncate table "+table+";"
            self.execute_query_on_db(query, sv_name, db_name)
    
    '''
    Input: Server name database name
    Output: drop all tables and then database, optionally disable foreign key constraints before dropping tables
    '''
    def drop_live_database(self,sv_name,db_name,disable_foreign_key_check=True):
        if disable_foreign_key_check==True:
            query="SET foreign_key_checks = 0;"
            self.__db_manager.execute_query(query, sv_name)
        
        tables=self.get_tables(sv_name,db_name)
        self.drop_tables(sv_name, db_name, tables)
        droplist=[]
        droplist.append(db_name)
        self.drop_databases(sv_name, droplist)
        
        if disable_foreign_key_check==True:
            query="SET foreign_key_checks = 1;"
            self.__db_manager.execute_query(query, sv_name)
    
    '''
    Input: server name, number of commands to skip
    Output: skips the provided number of commands on the slave and resumes replication
    '''
    def skip_and_resume(self,sv_name,commands_to_skip):
        query="stop slave;"
        self.__db_manager.execute_query(query, sv_name)
        query='SET GLOBAL SQL_SLAVE_SKIP_COUNTER='+str(commands_to_skip)+';'
        self.__db_manager.execute_query(query, sv_name)
        query="start slave;"
        self.__db_manager.execute_query(query, sv_name)
    
    '''
    Input: server name, database name, tables list, table schema
    Output: creating the tables in the database with the provided table schema
    '''
    def create_tables(self,sv_name,db_name,tables,schema):
        base_query="create table "
        for table in tables:
            query=base_query+table+' ('+schema+');'
            self.execute_query_on_db(query, sv_name, db_name)
    
    '''
    Input: server name, database list
    Output: creating the database on the server
    '''
    def create_databases(self,sv_name,databases):
        base_query="create database "
        for dbname in databases:
            query=base_query+dbname+';'
            self.__db_manager.execute_query(query, sv_name)
    
    '''
    Input: server name
    Output: returns true if replication is active else if replication is not active or not slave is not enabled return false 
    '''
    def check_replication(self,sv_name):
        query="show slave status;"
        status=self.__db_manager.execute_query(query, sv_name)
        if len(status)==0:
            return False
        else:
            if status[0]['Slave_IO_Running']=='Yes' and status[0]['Slave_SQL_Running']=='Yes':
                return True
            else:
                return False
    
    '''
    Input: server name
    Output: if mysql replication is not active, relay logs commands will be skipped until replication is resumed
    '''
    def skip_until_resumed(self,sv_name):
        while (self.check_replication(sv_name)==False):
            self.skip_and_resume(sv_name, 1)

    '''
    Input: server name
    Output: will return an array with the masters 
    '''
    def get_master_coordinates(self,sv_name):
        query="show master status;"
        coordinates=self.__db_manager.execute_query(query, sv_name)
        return coordinates[0]
    '''
    Input: slave server name, master server name
    Output: mysql replication will be stopped on slave, and coordinates would be adjusted to current master latest coordinates
    '''
    def synch_coordinates(self,sv_slave_name,sv_master_name):
        coordinates=self.get_master_coordinates(sv_master_name)
        replication_user=self.__db_manager.get_database_info()[sv_master_name][1]
        master_password=self.__db_manager.get_database_info()[sv_master_name][2]
        master_log_position=str(coordinates['Position'])
        master_log_file=coordinates['File']
        master_ip=sv_master_name
        query="stop slave;"
        self.__db_manager.execute_query(query, sv_slave_name)
        
        query="change master to MASTER_HOST='__master_host', MASTER_USER='__master_user', MASTER_PASSWORD='__master_password', MASTER_LOG_FILE='__master_log_file', MASTER_LOG_POS=__master_log_pos;"
        query=query.replace('__master_host', master_ip)
        query=query.replace('__master_user', replication_user)
        query=query.replace('__master_password', master_password)
        query=query.replace('__master_log_file', master_log_file)
        query=query.replace('__master_log_pos', master_log_position)
        
        self.__db_manager.execute_query(query, sv_slave_name)
        query="start slave;"
        self.__db_manager.execute_query(query, sv_slave_name)
        