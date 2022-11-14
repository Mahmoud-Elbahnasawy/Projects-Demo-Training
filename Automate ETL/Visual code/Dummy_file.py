#from Sql_queries import create_tables_list , insert_inTables_list , drop_tables_list
from Functions_module import *
import pyodbc

server = "DESKTOP-SUCDDNJ"
database_name = "master"
# conn = pyodbc.connect('DRIVER={SQL Server};\
#                        SERVER='+ server +';\
#                        DATABASE=' + database_name + ';\
#                        Trusted_Connection=yes')
# #define cursor 
# cur = conn.cursor()
# #table_creator(cur )
# query = """


# create database New_database
# """
# cur.execute(query)
# conn.commit()
# conn.close()

#print(float(('757,507,874').replace(",","")))
print(len("Agriculture, Forestry and Fishing Support Services"))
