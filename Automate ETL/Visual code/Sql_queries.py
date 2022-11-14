


filedata_table_create = """CREATE TABLE file_data (
                                                    file_id int identity(1,1) PRIMARY KEY,
                                                    file_name varchar(200),
                                                    date datetime,
                                                    type varchar(5),
                                                    size decimal(10,2),
                                                    case_check varchar(4)
                                                    );
                                                    """

industry_table_create = """CREATE TABLE industry (  Industry_aggregation_NZSIOC varchar(8) ,
                                                    Industry_code_NZSIOC varchar(150) PRIMARY KEY,
                                                    Industry_name_NZSIOC varchar(100)
                                                     )
                                                    """


survey_table_create = """CREATE TABLE survey (batch_id int ,
                                             total_rows_count int PRIMARY KEY,
                                             Year int,
                                             Industry_code_NZSIOC varchar(150),
                                             Units varchar(20),
                                             Variable_code varchar(3),
                                             Variable_name varchar(150),
                                             Variable_category varchar(40),
                                             value int,
                                             Industry_code_ANZSIC06 varchar(400),
                                             FOREIGN KEY (Industry_code_NZSIOC) REFERENCES industry(Industry_code_NZSIOC) )"""

rejected_table_create = """CREATE TABLE rejected (batch_id int,
                                             total_rows_count varchar(100) PRIMARY KEY,
                                             Year varchar(100),
                                             Industry_aggregation_NZSIOC varchar(8),
                                             Industry_code_NZSIOC varchar(5),
                                             Industry_name_NZSIOC varchar(100),
                                             Units varchar(20),
                                             Variable_code varchar(3),
                                             Variable_name varchar(100),
                                             Variable_category varchar(40),
                                             value varchar(50),
                                             Industry_code_ANZSIC06 varchar(400) )"""


# csv_table_create = """CREATE TABLE survey (batch_id Varchar(400) ,
#                                              total_rows_count Varchar(400),
#                                              Year Varchar(400),
#                                              Industry_aggregation_NZSIOC Varchar(400),
#                                              Industry_code_NZSIOC Varchar(400),
#                                              Industry_name_NZSIOC Varchar(400),
#                                              Units Varchar(400),
#                                              Variable_code Varchar(400),
#                                              Variable_name Varchar(400),
#                                              Variable_category Varchar(400),
#                                              value Varchar(400),
#                                              Industry_code_ANZSIC06 Varchar(400) )"""


                                                    
dtype_tabel_drop = """DROP TABLE survey"""
filedata_table_drop = """DROP TABLE file_data"""


file_data_tabel_insert = """INSERT INTO file_data (  file_name ,
                                                     date,
                                                     type,
                                                     size,
                                                     case_check) VALUES (?,GETDATE(),?,?,?)"""



industry_table_insert = """INSERT INTO industry
                                (Industry_aggregation_NZSIOC,
                                Industry_code_NZSIOC,
                                Industry_name_NZSIOC)
                                VALUES
                                (?,?,?)"""



survey_tabel_insert = """INSERT INTO survey (batch_id,
                                             total_rows_count,
                                             Year,
                                             Industry_code_NZSIOC,
                                             Units,
                                             Variable_code,
                                             Variable_name,
                                             Variable_category,
                                             value,
                                             Industry_code_ANZSIC06) VALUES (?,?,?,?,?,?,?,?,?,?)"""
                                    

rejected_tabel_insert = """INSERT INTO rejected (batch_id,
                                             total_rows_count,
                                             Year,
                                             Industry_aggregation_NZSIOC,
                                             Industry_code_NZSIOC,
                                             Industry_name_NZSIOC,
                                             Units,
                                             Variable_code,
                                             Variable_name,
                                             Variable_category,
                                             value,
                                             Industry_code_ANZSIC06) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"""


create_tables_list = [filedata_table_create ,industry_table_create, survey_table_create , rejected_table_create]
insert_inTables_list = [file_data_tabel_insert]
drop_tables_list = [filedata_table_drop]


#connection_open()