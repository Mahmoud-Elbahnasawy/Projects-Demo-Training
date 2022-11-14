import csv
import datetime
import json
import os
import pathlib  # for checking the extension of a file
import shutil  # used in moving directories
import time
import uuid

import pandas as pd
import schedule

from Sql_queries import *

# defining varaibles for the orogin and target and recycle bin directories
origin = 'F:\\ETL_telecom_usecase\\Chunked_Csv_files'
target = 'F:\\ETL_telecom_usecase\\Received_chunked_from_source'
accepted = 'F:\\ETL_telecom_usecase\\Accepted'
rejected_Path = 'F:\\ETL_telecom_usecase\\Rejected'



def source_file_name_list(origin = origin):
    """
    This function checks for files already existed in source folder and their number
    """
    source_file_name_list = os.listdir(origin)
    number_of_files = len(source_file_name_list)
    return source_file_name_list , number_of_files


def dest_file_name_list(target = target):
    """
    This function checks for files already existed in target folder and their number
    """
    dest_file_name_list = os.listdir(target)
    dest_number_of_files = len(dest_file_name_list)
    return dest_file_name_list , dest_number_of_files







def time_calculator(increment_in_seconds = 1 , start_time = datetime.datetime.now()):
    """
    define a function to secify the times in which the file will move from source to destination
    """
    # for example if we call this function now the first time will be after a certain time in seconds from now
    time_amount_after_which_we_start = 2
    time_list = []
    #time_list.clear()
    number_of_files = source_file_name_list()[1]
    for i in range(number_of_files):
        increment = datetime.timedelta(seconds = ((i * increment_in_seconds) + time_amount_after_which_we_start))
        # Note add 2 seconds so the first time we send a file from source to destination in after now by two minutes
        
        #defining a variable in which we store the time value
        calculated_time_at_which_file_will_be_sent = start_time+increment
        
        # appending the value in the list and excluding microseconds and seconds
        time_list.append(calculated_time_at_which_file_will_be_sent.replace(microsecond=0))
    return time_list




def sender(file ,  origin  = origin, target = target):
    """
    this function sends the file to the destination
    """
    try:   
        shutil.move(origin +"\\"+file , target)
    except Exception as e:
        print(e)
    return 1


def check_if_file_exists (file):
    """
    This function checks if the file is existed or not in the destination
    """
            
    #define a list to hold destination file
    destination_files_list = dest_file_name_list()[0]

    # this loop iterates through the destination files list to chekc if the file exists or not
    for j in destination_files_list:
        if (j == file):
            #existed = 1
            
            print("The file is already existed why you want to move it once again\n\n\n")
            break
        else : 
            sender(origin , file , target)


transport_log = [("origin_name","dest_name","id","time_key","status")]
def check_file_move (file , counter ):
    """
    This function checks If file has been succesfully moved or not
    """
    time_key = datetime.datetime.now().replace(microsecond=0,second = 0)
    if (dest_file_name_list(target)[0][counter] == file):
        print(" i have checked it and everything is succesfully done")
        status = True
        transport_log.append((file,dest_file_name_list(target)[0][counter], counter+1 , time_key , status))
         
    else :
        # if the file was not properly moved
        status = False
        transport_log.append((file,dest_file_name_list(target)[0][counter], counter+1 , time_key , status))
    print(transport_log)
    #return transport_log


def main_mover():

    """
    """
    list_of_source_file_names = source_file_name_list()[0]
    source_file_count = source_file_name_list()[1]

    time_calculator(1,datetime.datetime.now())

    time.sleep(1.9)
    # while loop to check when time is correct it moves one file
    current_time = datetime.datetime.now().replace(microsecond=0)
    transport_log = [("origin_name","dest_name","id","time_key","status")]
    

    # define a counter
    counter = 0
    time_list = time_calculator()
    while (current_time <= time_list[-1] and counter < source_file_count ):
     
        time.sleep(1)
   
        # while the time now is less that the last time at which we have to send a file (do the following job)
    
    
        # here we have to possible events
        # the first is that the time is not at which we perform sending a file
    
        if (current_time != time_list[counter]):
            pass
        else :
        
            # defining a variable to know which file has it's turn to be moved now
            file  = list_of_source_file_names[counter]
        
            #define a variable to store true if the file already existed and false if not
            existed = False
        
            # first we want to check that this file is not existed in the destination before
            
                   
            sender(file , origin , target)            
            #check_if_file_exists(file)
        
        
            # the following block of code will send the file if it was not existe ( existed = False )
            if (existed == False):
                print("The file is not existed we have to move it to destination")

                # execution of the function
                

                # execution of the function
                check_file_move(file , counter)
            
            if (counter < source_file_count):
                counter += 1

        current_time = datetime.datetime.now().replace(microsecond=0)
    
    
    print(f"we stope at {datetime.datetime.now().replace(microsecond=0)} and the last item in time list is {time_list[-1]}")
    




# i want to define a function that grabs every file being transported into the staging area




















    


# this function gets file name



# this function get file_size
def file_size(file):
    """
    This function gets the file size in KB
    """
    file_with_path = target +"\\" + file
    file_size = (os.stat(file_with_path).st_size)/1000
    max_file_size_KB = 700    
    if (file_size > max_file_size_KB):
        size_check_case = False
    else :
        size_check_case = True
    #print(file_size)
    return file_size , size_check_case



def check_type(file):
    """
    this function checks the file type
    """
    file_with_path = target +"\\" + file
    type_check_case = True
    file_type =  pathlib.Path(file_with_path).suffix
            
    try:
        # check if the file extension is as needed or not
        assert file_type == ".csv"
        #print(file_type)
    except Exception as e:
        # if it is not a csv file the set type_check_case to false
        type_check_case = False
        #print(e)
    return file_type , type_check_case



def loader(file):
    if (check_type(file)):
        df = pd.read_csv(file)
        df.rename(columns = {'Unnamed':'Total_rows_counter'}, inplace = True)
    return df

#def remover(file_name):
#    sender( file_name, target , recycle_bin_path )


def column_number(file):
    df = loader(file)
    columns = df.shape[1]
    print("Columns",columns)
    return columns


def row_number(file):
    df = loader(file)
    rows = df.shape[0]
    print("Rows",rows)
    return rows


def column_date_types(file):
    columns_types_list=[]
    df = loader(file)
    for i in df.dtypes:
        columns_types_list.append(i)
    return columns_types_list


def null_counter(file):
        null_list=[]
        df = loader(file)
        for i in df.isnull().sum():
            null_list.append(i)
        print("Nulls",null_list)


def duplicated_counter(file):
    df = loader(file)
    duplicates = df.duplicated().sum()
    print("Duplicates:-",duplicates)
    return duplicates







def insert_file_data(file,conn,cur,index):
    with open(target+"\\"+file) as csvfile:
        
        readCSV = csv.reader(csvfile, delimiter=',')
        survey_table_counter = 0
        rejected_table_counter = 0
        # to skip the first row(header)
        next (readCSV,None)
        for row in readCSV:
            if row:
                try:

                    # inserting data into industry_table
                    cur.execute(industry_table_insert ,[row[2],row[3],row[4]])
                    conn.commit()
                except Exception as e:
                    pass
 
    with open(target+"\\"+file) as csvfile:
        
        readCSV = csv.reader(csvfile, delimiter=',')
        survey_table_counter = 0
        rejected_table_counter = 0
        # to skip the first row(header)
        next (readCSV,None)

        for row in readCSV:
            if row :
                try: 
                    total_rows_count = int(row[0])
                    year = int(row[1])
                    # this value contain "," instead of a "." so we have to replace it
                    value = int((row[9]).replace(",",""))
                    cur.execute(survey_tabel_insert,
                    [index,
                    total_rows_count,
                    year,
                    row[3],
                    row[5],
                    row[6],
                    row[7],
                    row[8],
                    value,
                    row[10]])
                    conn.commit()
                    survey_table_counter += 1
                    print(survey_table_counter)
                
                except Exception as e:
                    #print(e)
                    

                    cur.execute(rejected_tabel_insert,
                    [index,
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                    row[8],
                    row[9],
                    row[10]])
                    conn.commit()
                    rejected_table_counter +=1
                    print(rejected_table_counter)

                    
        


def rejected_or_passed(conn,cur,file,dest_list_of_files,index ,size,type):

    """
    THIS FUNCTION WILL SEND FILE TO BE PRCESSED OR REJECTS THEM"""
    if ((file_size(file)[0] <= 700) and (check_type(file)[0] == ".csv")):
        # takes the received file from to the accepted folder
        
        #print("TRY sutvey data..")
        insert_file_data(file , conn,cur,index)

        sender(file , target , accepted)
        case = 'pass'
        # cur.execute( filedata_table_create)
        # #table_creator(cur)
        # conn.commit()
        # cur.execute( csv_table_create )
        # conn.commit()

        cur.execute( file_data_tabel_insert ,(dest_list_of_files[index] , type , size , "fass"))
        #print("succeded file_Data")
        conn.commit()


    else : 
        # takes the rejected file to the rejected folder
        sender(file , target , rejected_Path)
        cur.execute( file_data_tabel_insert ,(dest_list_of_files[index] , type , size , "fail"))
        case = 'fail'
    return case




def table_creator(conn,cur):
    """THIS FUNCTION CREATES NEEDED TABLES"""
    for i in create_tables_list :
        
        try:
            cur.execute(i)
            conn.commit()
        except Exception as e:
            print(e)
        #print("after executing table creation")
        







# def main_checker_and_distributer(conn , cur):
#     """THIS FUNCTION CHECKS FOR DATA IN STAGING AREA AND ACCEPTS OR REJECTS FILES Then loads files and files data into database"""
#     print("Tring to create tables")
#     try:
#         table_creator(conn,cur)
#     except Exception as e :
#         print("The tables may be already existed")

#     number_of_checks = 1
#     while (number_of_checks <5) :

#         dest_list_of_files = []
#         try :
#             dest_number_of_files = dest_file_name_list()[1]
#             if (dest_number_of_files>=1) :
#                 dest_list_of_files = dest_file_name_list()[0]
#                 for index , name in enumerate(dest_list_of_files):
                    
                
#                     # getting file size
#                     size = file_size(name)[0]

#                     # getting file type
#                     type = check_type(name)[0]

#                     # check if the file is rejected or accepted
#                     case = rejected_or_passed(conn,cur,name,dest_list_of_files,index,size)


                    

#                     # we need to to insert data in database


#             number_of_checks += 5

#         except Exception as e :
#             print(e)


#             # we will not count one more check exept we don't find files
#             print(e)
#             number_of_checks += 5
#             time.sleep(2)
#         print(target)





    


        















































def main_checker_and_distributer(conn , cur):
    """THIS FUNCTION CHECKS FOR DATA IN STAGING AREA AND ACCEPTS OR REJECTS FILES Then loads files and files data into database"""
    print("Trying to create tables")
    try:
        table_creator(conn,cur)
    except Exception as e :
        print("The tables may be already existed")

    number_of_checks = 1
    while (number_of_checks <5) :

        dest_list_of_files = []
        
        dest_number_of_files = dest_file_name_list()[1]
        if (dest_number_of_files>=1) :
            dest_list_of_files = dest_file_name_list()[0]
            for index , name in enumerate(dest_list_of_files):
                    
                
                # getting file size
                size = file_size(name)[0]

                # getting file type
                type = check_type(name)[0]

                # check if the file is rejected or accepted
                case = rejected_or_passed(conn,cur,name,dest_list_of_files,index,size,type)


                    

        else:
            print("No files")        # we need to to insert data in database


        number_of_checks += 5



            # we will not count one more check exept we don't find files
            
        number_of_checks += 5
        time.sleep(2)
    print(target)




















































































































































































def has_any_file_arrived_in_staging_area():
    counter = 0
    fetched_file_name = dest_file_name_list()[0][counter]
    fetched_file_with_path = target +"\\"+fetched_file_name
    
    # define a variable to hold size_check_case and setting its default to be true
    size_check_case = True
    print("F:-",fetched_file_name)    
        
    # the execution of the function
    file_size(fetched_file_with_path)
            
    # define a variable to holde the type_check case
                    
    # the execution of the function        
    check_type(fetched_file_with_path)
            
# note that i have to delete this file after reading it (for simplicity) and so that not to search for it again
    
    # defining a function to count columns
      
    #function execution
    column_number(fetched_file_with_path)
        
    # defining a function to count rows
       
    #function execution
    row_number(fetched_file_with_path)
             
    print(column_date_types(fetched_file_with_path))  
        
    column_date_types(fetched_file_with_path)
          
    null_counter(fetched_file_with_path)
    
    #excution of the function
    duplicated_counter(fetched_file_with_path)
    
    
    # the execution of the function   
    #remover(fetched_file_name)
    counter+=1
    print("\n\n\n")

















































































































































































































































def starter():
    """
    This function Looks at received folder for files
    If it found a file it takes it to be transformed and moves it to recycled_bin 
    if it did not find a file it rechecks agian for possible comming files
    """
    while True:
        time.sleep(0.9)
        #fetched_file_name = dest_file_name_list()[0][0]
        try:
            number_of_files_in_destination = dest_file_name_list()[1]
        except Exception as e:
            number_of_files_in_destination = 0
        if (number_of_files_in_destination >0):
            fetched_file_name = dest_file_name_list()[0][0]
            fetched_file_with_path = target +"\\"+fetched_file_name
            
            has_any_file_arrived_in_staging_area()

