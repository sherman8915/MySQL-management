'''
Created on Dec 13, 2012

@author: sherman

'''
import MySQLdb
import sys
import datetime
import time

class mysql_query_manager(object):
    '''
    Description: manages mysql query execution on mysql databases
    '''
    __database_cursors={}
    __database_connections={}
    __database_info={}
    
    __log_files={'executed':'./queries_executed'}
    
    def __init__(self,databases):
        '''
        Input: a database doctionary of the format dbname:address
        Output: instantiating a connection and a cursor to each one of the databases
        '''
        
        self.__database_info=databases
        
        for sv_name in self.__database_info.keys():
            dbname=self.__database_info[sv_name][0]
            username=self.__database_info[sv_name][1]
            password=self.__database_info[sv_name][2]
            self.__get_db_cursor(sv_name,dbname,username,password)
        
        self.__create_log_files()
    '''
    Input: database credential and hostname
    Output: db cursor
    '''
    def __get_db_cursor(self,sv_name,sv_hostname,username,password):
        try:
            db = MySQLdb.connect(sv_hostname,username,password)
        except MySQLdb.Error as e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            sys.exit (1)
        cursor = db.cursor(MySQLdb.cursors.DictCursor)
        self.__database_connections[sv_name]=db
        self.__database_cursors[sv_name]=cursor
        return cursor
    
    
    '''
    Input: query string, database name
    Output: executing the provided query for the provided database name, returns the resulted database output
    '''
    def execute_query(self,query,sv_name,repeat_on_error=False,stop_on_error=False):
        try:
            statement=sv_name+": "+query
            self.__log(statement,self.__log_files['executed'])
            self.__database_cursors[sv_name].execute(query)
            rows=self.__database_cursors[sv_name].fetchall()
            return rows
        except MySQLdb.Error as e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            if repeat_on_error==True:
                worked=False
                while worked==False:
                    try:
                        time.sleep(1)
                        statement=sv_name+": "+query
                        self.__log(statement,self.__log_files['executed'])
                        self.__database_cursors[sv_name].execute(query)
                        rows=self.__database_cursors[sv_name].fetchall()
                        worked=True
                        return rows
                    except MySQLdb.Error as e:
                        worked=False
                        print "Error %d: %s" % (e.args[0], e.args[1])        
            else:
                print "Error executing query: "+query
                if stop_on_error==True:
                    sys.exit (1) 
        return 0
    
    
    '''
    Input: void
    Output: closing all open connections
    '''
    def close_connections(self):
        for conn in self.__database_connections.keys():
            self.__database_connections[conn].close()
    
        '''
    Input: statement to log, path to log file
    Output: logs the statement in the provided file path
    '''
    def __log(self,statement,file):
        f=open(file,'a')
        now=datetime.datetime.now()
        strnow=now.strftime("%x %X ----")
        f.write(strnow+": "+statement+"\n")
        f.close()
    
    '''
    creates log files
    '''         
    def __create_log_files(self):
        for file in self.__log_files.values():
            f=open(file,'w')
            f.close()
    
    def get_database_info(self):
        return self.__database_info