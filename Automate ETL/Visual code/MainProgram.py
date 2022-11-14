import pyodbc

#from Sql_queries import create_tables_list , insert_inTables_list , drop_tables_list
from Functions_module import *

server = "DESKTOP-SUCDDNJ"
database_name = "ETL_siParadigm"


# server = "DESKTOP-SUCDDNJ"
# database_name = "ETL_siParadigm"
# conn = pyodbc.connect('DRIVER={SQL Server};\
#                        SERVER='+ server +';\
#                        DATABASE=' + database_name + ';\
#                        Trusted_Connection=yes')

#define cursor 
#cur = conn.cursor()
def main():
    
    
    
    #main_mover()

    conn = pyodbc.connect('DRIVER={SQL Server};\
                       SERVER='+ server +';\
                       DATABASE=' + database_name + ';\
                       Trusted_Connection=yes')
    #define cursor 
    cur = conn.cursor()

    #cur.execute(dtypes_table_create)
    #cur.execute(filedata_table_create)
    
    #cur.execute(file_data_tabel_insert)

    #cur.execute(filedata_table_drop)
    

    
    main_checker_and_distributer(conn,cur)
    
   



    conn.close()



if __name__ == "__main__":
    
    main()


    





