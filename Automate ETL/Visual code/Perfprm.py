from Functions_module import *

origin = 'F:\\ETL_telecom_usecase\\Chunked_Csv_files'
target = 'F:\\ETL_telecom_usecase\\Received_chunked_from_source'

file = source_file_name_list()[0][0]
print(sender(origin , file,target ))

