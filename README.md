A simple utility class in python I wrote for managing a replicating live MySQL database server/s

The manages the cursor and connection in the mysql_conn_manager class and provide some useful api's for routine tasks through the utility manager class.

Imagine that you have master and slave that replicate in a live production environment, assume that you wish to execute a command on the master such as "drop database" that has a very high chance of breaking replication to the slave.

you can issue that command and immediately after it you can use: skip_and_resume

Example:

#example.py
import mysql_utility_manager

##database info is a key values dictionary of the form: server_name:[ip_address/fqdn,username,password]
database_info={'read_write_master':['master.domain.net','root','rw1 password'],
               'read_only_slave':['slave.domain.net','root','ro1 password'],


#instantiate the utility manager object for mysql
db_util_manager=mysql_utility_manager.utility_manager(database_info) 

bad_query="do something that will generate error on slave and will break replication"

#the command will be executed on the master and will break replication on the slave
db_util_manager.execute_query_on_db(bad_query,'read_write_master','my_database')

# It will check on the slave whether replication is running, if the replication is not running it will skip any offending commands in the binary log until replication has resumed.
# Thus minimizing downtime for unexpected errors during live maintainance.
db_util_manager.skip_and_resume('read_only_slave')

