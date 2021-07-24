import os, os.path
import math
import pandas as pd
# from multiprocessing import Pool
import pyodbc
import uuid 
from datetime import datetime,date

cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
                      "Server=172.20.132.19;"
                      "Database=NFSA;"
                      "Trusted_Connection=no;"
                      "UID=sa;"
                      "PWD=admin@123")
# cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
#                       "Server=.;"
#                       "Database=NFSA_COPY;"
#                       "Trusted_Connection=no;"
#                       "UID=sa;"
#                       "PWD=123456")
# cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
#                       "Server=.;"
#                       "Database=NFSA_COPY;"
#                       "Trusted_Connection=no;"
#                       "UID=sa;"
#                       "PWD=123456")
district_list=[ 
'D_327_DARJEELING_1'
,'D_328_JALPAIGURI_1'
,'D_329_COOCHBEHAR_1'
,'D_330_UTTAR_DINAJPUR_1'
,'D_331_DAKSHIN_DINAJPUR_1'
,'D_332_MALDAH_1'
,'D_333_MURSHIDABAD_1'
,'D_334_BIRBHUM_1'
,'D_335_Purba_Bardhaman_1'
,'D_336_NADIA_1'
,'D_337_NORTH_24PGS_1'
,'D_338_HOOGHLY_1'
,'D_339_BANKURA_1'
,'D_340_PURULIA_1'
,'D_341_HOWRAH_1'
,'D_342_Kolkata_1'
,'D_343_SOUTH_24PGS_1'
,'D_344_PASCHIM_MEDINIPUR_1'
,'D_345_PURBA_MEDINIPUR_1'
,'D_346_Alipurduar_1'
,'D_347_Paschim_Bardhaman_1'
,'D_348_JHARGRAM_1'
,'D_349_KALIMPONG_1'
]

# def getdata_and_upload_to_server(DIR):            
#     for name in os.listdir(DIR):
#         if name.endswith('.csv'):
#             df=pd.read_csv(name)
#             # Get Data from Death CSV
#             print('Fetching And Processing Data from File: '+name)
#             starttime=datetime.now()
#             dfDeathData = pd.read_csv(name)
#             dfDeathData=dfDeathData.drop(['DEATH_SPOUSE_NAME', 'DEATH_DATE','DEATH_BASE_REGN_DATE','DEATH_AGE_YEAR','DEATH_SEX'], axis=1)
#             dfDeathData['Full_Address']=dfDeathData[dfDeathData.columns[2:]].apply(
#                 lambda x: ','.join(x.dropna().astype(str)),
#                 axis=1
#             )
#             dfDeathData['DEATH_DECEASED_NAME']=dfDeathData['DEATH_DECEASED_NAME'].str.replace('[^\w\s]','')
#             dfDeathData['DEATH_FATHER_NAME']=dfDeathData['DEATH_FATHER_NAME'].str.replace('[^\w\s]','')
#             dfDeathData=dfDeathData.drop(['DEATH_RESIDENCE', 'DEATH_RESIDENCE_STREET','DEATH_RESIDENCE_PREMISES','DEATH_RESIDENCE_DIST','DEATH_RESIDENCE_STATE'], axis=1)
#             endtime=datetime.now()
#             difference = endtime - starttime
#             print('Fetching And Processing Data Of '+name+' Complete In '+calculate_hours_minutes(difference)+'...')
#             print("No Of Records in "+name+" Is : "+str(len(df.index)))
#             starttime=datetime.now()
#             print('Loading To server...')
#             cursor = cnxn.cursor()
#             # Insert Dataframe into SQL Server:
#             for index, row in dfDeathData.iterrows():
#                 cursor.execute("INSERT INTO tbl_death_data_main(death_name,death_father_name,death_address,file_name) values(?,?,?,?)", str(row.DEATH_DECEASED_NAME), str(row.DEATH_FATHER_NAME), str(row.Full_Address),str(name))
#             cnxn.commit()
#             cursor.close()
#             endtime=datetime.now()
#             difference = endtime - starttime
#             print('Loading To server Complete IN '+calculate_hours_minutes(difference)+'...')
#             # print('Removing File: '+name)
#             # os.remove(name)
#             print(name+' File Removed')

# Creating For Getting Time Differences.
def calculate_hours_minutes(td):
    seconds=int(td.total_seconds())
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return str(hours)+' Hour '+str(minutes)+' Minutes ' +str(seconds)+' Seconds'

def get_Death_data_deatils_ChunkWise():
    ## Save All the Data From Server To Individual CSVs
    print('Fetching and Breaking Uploaded Death Data From Server...')
    log_into_text_file('Fetching and Breaking Uploaded Death Data From Server...')
    starttime=datetime.now()
    cursor = cnxn.cursor()
    dfMaxdata = pd.read_sql_query('select COUNT(id) as MaxNumber from tbl_death_data_main where is_send=0', cnxn)
    data_in_each_csv=100
    noofloop=math.ceil(dfMaxdata.MaxNumber/data_in_each_csv)
    cmd_prod_executesp = "exec sp_get_Death_data_deatils_ChunkWise"
    outdir = './CSV_FROM_SERVER'
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    for x in range(noofloop):
        print('Creating File '+str(x+1)+' of '+str(noofloop))
        log_into_text_file('Creating File '+str(x+1)+' of '+str(noofloop))
        df1=pd.read_sql_query(cmd_prod_executesp, cnxn)
        filename=str(uuid.uuid4().hex[:9].upper())+'.csv'
        filename = os.path.join(outdir, filename)
        df1.to_csv(filename)
        v=df1['death_main_id'].tolist()
        v=",".join(map(str,v))
        sql="update tbl_death_data_main set is_send=1 where id in (select val from dbo.f_split('"+v+"',','))"
        # print(sql)
        cursor.execute(sql)
        cnxn.commit()    
    cursor.close()
    endtime=datetime.now()
    difference = endtime - starttime
    print('End of Fetching and Breaking Uploaded Death Data From Server Complete In '+calculate_hours_minutes(difference)+'...\n')
    log_into_text_file('End of Fetching and Breaking Uploaded Death Data From Server Complete In '+calculate_hours_minutes(difference)+'...\n')

def get_all_rc_data_as_on_date(district_table):
        print('Fetching Data from RC Server For District: '+ str(district_table))
        log_into_text_file('Fetching Data from RC Server For District: '+ str(district_table))
        starttime=datetime.now()
        dfRCDataMain = pd.read_sql_query('select PERMANENT_RCNO,ISNULL(NAME,\'\') AS NAME,ISNULL(FATHERNAME,\'\') AS FATHERNAME,BlockMunicipalityCode,RCCATEGORY from '+str(district_table)+' (nolock)', cnxn)
        dfRCDataMain['death_name_matching_score'] = 0
        dfRCDataMain['death_father_name_matching_score'] = 0
        dfRCDataMain['death_pin_matching_score'] = 0
        dfRCDataMain['death_block_matching_score'] = 0
        dfRCDataMain.to_csv("tbl_"+district_table+".csv",columns=['PERMANENT_RCNO','NAME','FATHERNAME','BlockMunicipalityCode','RCCATEGORY','death_name_matching_score','death_father_name_matching_score','death_pin_matching_score','death_block_matching_score'])
        endtime=datetime.now()
        difference = endtime - starttime
        print('Fetching Data from RC Server For District: '+ str(district_table)+' Complete In '+calculate_hours_minutes(difference)+'...\n')
        log_into_text_file('Fetching Data from RC Server For District: '+ str(district_table)+' Complete In '+calculate_hours_minutes(difference)+'...\n')
        

def get_block_muni_pin_data_from_db():
    #Fetch data from Ration Card Database
    print('Fetching Block, Municipality and Pincode Data from RC Server...\n')
    log_into_text_file('Fetching Block, Municipality and Pincode Data from RC Server...\n')
    starttime=datetime.now()        
    dfRCBlockData = pd.read_sql_query('select * from BlockMunicipalityCombineMaster', cnxn)
    dfRCPinData = pd.read_sql_query('select distinct Pincode  from Pincode_Master', cnxn)
    if not os.path.exists("Block.csv"):        
        dfRCBlockData.to_csv("Block.csv")
    else:
        os.remove("Block.csv")
        dfRCBlockData.to_csv("Block.csv")

    if not os.path.exists("PIN.csv"):        
        dfRCPinData.to_csv("PIN.csv")
    else:
        os.remove("PIN.csv")
        dfRCPinData.to_csv("PIN.csv")
    endtime=datetime.now()
    difference = endtime - starttime
    print('Fetching Data from RC Server Complete In '+calculate_hours_minutes(difference)+'...\n')
    log_into_text_file('Fetching Data from RC Server Complete In '+calculate_hours_minutes(difference)+'...\n')

def log_into_text_file(message):
    todaydate=str(date.today())+".txt"
    if not os.path.exists(todaydate):        
        with open(todaydate, 'w') as fp:
            pass
    
    f = open(todaydate, "a")
    f.write('\n'+message)
    f.close()

# def fetch_execute():
#     log_into_text_file('Started Processing From File Collect Data From RC:....')
#     get_block_muni_pin_data_from_db()
#     get_Death_data_deatils_ChunkWise()    
#     noofdist=len(district_list)
#     with Pool(noofdist) as pdist:
#         pdist.map(get_all_rc_data_as_on_date, district_list)
#     log_into_text_file('End of Processing From File Collect Data From RC:....')

# if __name__ == '__main__':
#     # path = askdirectory(title='Select Folder') # shows dialog box and return the path
#     # getdata_and_upload_to_server(path)
#     log_into_text_file('Started Processing From File Collect Data From RC:....')
#     get_block_muni_pin_data_from_db()
#     get_Death_data_deatils_ChunkWise()    
#     noofdist=len(district_list)
#     with Pool(noofdist) as pdist:
#         pdist.map(get_all_rc_data_as_on_date, district_list)
#     log_into_text_file('End of Processing From File Collect Data From RC:....')
    