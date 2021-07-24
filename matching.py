import os, glob
from pathlib import Path
from datetime import datetime,date,time
import pandas as pd
import pyodbc
import math
from fuzzywuzzy import fuzz
import fuzzy_pandas as fpd
import numpy as np
import csv

cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
                      "Server=172.20.132.19;"
                      "Database=NFSA;"
                      "Trusted_Connection=no;"
                      "UID=sa;"
                      "PWD=admin@123")

# cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
#                       "Server=172.20.132.19;"
#                       "Database=NFSA;"
#                       "Trusted_Connection=no;"
#                       "UID=sa;"
#                       "PWD=admin@123")
# cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
#                       "Server=.;"
#                       "Database=NFSA_COPY;"
#                       "Trusted_Connection=no;"
#                       "UID=sa;"
#                       "PWD=123456")

# Clean a string 
def cleanName(series):
    return  series.str.lower().str.replace(r"[^a-z\s]",repl="",regex=True).str.split().str.join(" ").str.strip()

#Creating Log Functaionalities
def log_into_text_file(message):
    todaydate=str(date.today())+".txt"
    if not os.path.exists(todaydate):        
        with open(todaydate, 'w') as fp:
            pass
    
    f = open(todaydate, "a")
    f.write('\n'+message)
    f.close()

# Createing Resultant Files
def Creating_result_files():
    BlockMatchedFileName="BlockMatched_"+str(date.today())+".csv"
    PinMatchedFileName="PinMatched_"+str(date.today())+".csv"
    NameMatchedFileName="NameMatched_"+str(date.today())+".csv"
    BlockFileHeader=['death_main_id','blockcode']
    PinFileHeader=['death_main_id','pincode']
    NameFileHeader=['death_main_id','PERMANENT_RCNO','NAME','FATHERNAME','BlockMunicipalityCode','RCCATEGORY']
    if not os.path.exists(BlockMatchedFileName):        
        with open(BlockMatchedFileName, 'w') as fp:
            writer = csv.writer(fp)
            writer.writerow(BlockFileHeader)
            pass
    
    if not os.path.exists(PinMatchedFileName):        
        with open(PinMatchedFileName, 'w') as fp:
            writer = csv.writer(fp)
            writer.writerow(PinFileHeader)
            pass
    
    if not os.path.exists(NameMatchedFileName):        
        with open(NameMatchedFileName, 'w') as fp:
            writer = csv.writer(fp)
            writer.writerow(NameFileHeader)
            pass

#Creating Block Final Reslut Files with Data
def WritetoCSVFileBlockMatching(dfRcdata):
    BlockMatchedFileName="BlockMatched_"+str(date.today())+".csv"
    dfRcdata.to_csv(BlockMatchedFileName, mode = 'a', header = False, index = False)
    


# Creating For Getting Time Differences.
def calculate_hours_minutes(td):
    seconds=int(td.total_seconds())
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return str(hours)+' Hour '+str(minutes)+' Minutes ' +str(seconds)+' Seconds'

# Creating function for Matching Name col_name_to_be_matched,col_name_to_matched_with--> Dataframe Column Name
def get_ratio(row,col_name_to_be_matched,col_name_to_matched_with):
    name=  row[col_name_to_be_matched]  #'to_be_matched'
    name1 = row[col_name_to_matched_with] # 'NAME'
    return fuzz.token_set_ratio(name, name1)


def read_and_process_csv_for_block(filename):
    cwd = os.getcwd()
    path1=Path(os.path.join(cwd, "CSV_FROM_SERVER"))
    dfDeathData = pd.read_csv(path1.joinpath(filename),dtype = str)
    dfRCDataMain=pd.read_csv('BLOCK.csv',dtype = str)
    for ind in dfDeathData.index:
        print('Start Processing Data no: '+str(ind+1)+' of file: '+ str(filename))    
        log_into_text_file('Start Processing Data no: '+str(ind+1)+' of file: '+ str(filename))    
        starttime=datetime.now()
        dfRCData=dfRCDataMain
        dfRCData['death_address']=str(dfDeathData['death_address'][ind])
        print('Looking To match For BlockName In Death Address: '+str(dfDeathData['death_address'][ind])+' of file: '+ str(filename))
        log_into_text_file('Looking To match For BlockName In Death Address: '+str(dfDeathData['death_address'][ind])+' of file: '+ str(filename))
        dfRCData['death_block_matching_score']=dfRCData.apply(get_ratio,col_name_to_be_matched='death_address',col_name_to_matched_with='BlockName', axis=1)
        dfRCData_level_1_filter=dfRCData.loc[(dfRCData['death_block_matching_score'] >= 80)].copy()
        
        if len(dfRCData_level_1_filter.index)>0:
            cursor = cnxn.cursor()
            # Insert Dataframe into SQL Server:
            for index, row in dfRCData_level_1_filter.iterrows():
                retry_flag = True
                retry_count = 0
                while retry_flag and retry_count < 5:
                    try:                        
                        cursor.execute("INSERT INTO tbl_death_matching_block(death_main_id,block_id) values(?,?)", int(dfDeathData['death_main_id'][ind]), str(row.BlockCode))
                        cnxn.commit()
                        retry_flag = False
                    except:                        
                        print ("Retry after 1 sec: Form Block Matching")
                        retry_count = retry_count + 1
                        time.sleep(1)
            cursor.close()
        else:
            print('Matching Failed For Block Name...of file: '+ str(filename))
            log_into_text_file('Matching Failed For Block Name...of file: '+ str(filename))
        endtime=datetime.now()
        difference = endtime - starttime
        print('End Processing Data no: '+str(ind+1)+'  In '+calculate_hours_minutes(difference)+' of file: '+ str(filename))
        log_into_text_file('End Processing Data no: '+str(ind+1)+'  In '+calculate_hours_minutes(difference)+' of file: '+ str(filename))
        # print('Generating Result For Name: '+str(dfDeathData['DEATH_DECEASED_NAME'][ind])+' And Father Name: '+dfDeathData['DEATH_FATHER_NAME'][ind]+'.. \n')
        # dfResult=dfRCData.loc[((dfRCData['death_name_matching_score'] >= 90) & (dfRCData['death_father_name_matching_score'] >= 90))]
        # print('We Have :'+str(len(dfResult.index))+' Matching Rows For Name: '+str(dfDeathData['DEATH_DECEASED_NAME'][ind])+' And Father Name: '+str(dfDeathData['DEATH_FATHER_NAME'][ind])+'.. \n')

def read_and_process_csv_for_pin(filename):
    cwd = os.getcwd()
    path1=Path(os.path.join(cwd, "CSV_FROM_SERVER"))
    dfDeathData = pd.read_csv(path1.joinpath(filename),dtype = str)
    dfRCDataMain=pd.read_csv('PIN.csv',dtype = str)
    for ind in dfDeathData.index:
        print('Start Processing Data no: '+str(ind+1)+' of file: '+ str(filename))    
        log_into_text_file('Start Processing Data no: '+str(ind+1)+' of file: '+ str(filename))    
        starttime=datetime.now()
        dfRCData=dfRCDataMain
        dfRCData['death_address']=str(dfDeathData['death_address'][ind])
        print('Looking To match For Pin In Death Address: '+str(dfDeathData['death_address'][ind])+' of file: '+ str(filename))
        log_into_text_file('Looking To match For Pin In Death Address: '+str(dfDeathData['death_address'][ind])+' of file: '+ str(filename))
        dfRCData['death_pin_matching_score']=dfRCData.apply(get_ratio,col_name_to_be_matched='death_address',col_name_to_matched_with='Pincode', axis=1)
        dfRCData_level_1_filter=dfRCData.loc[(dfRCData['death_pin_matching_score'] >= 80)].copy()
        
        if len(dfRCData_level_1_filter.index)>0:
            cursor = cnxn.cursor()
            # Insert Dataframe into SQL Server:
            for index, row in dfRCData_level_1_filter.iterrows():
                retry_flag = True
                retry_count = 0
                while retry_flag and retry_count < 5:
                    try:
                        
                        cursor.execute("INSERT INTO tbl_death_matching_pinno(death_main_id,matched_pin) values(?,?)", int(dfDeathData['death_main_id'][ind]), str(row.Pincode))
                        cnxn.commit()
                        retry_flag=False
                    except:
                        print ("Retry after 1 sec: Form Pin Matching")
                        retry_count = retry_count + 1
                        time.sleep(1)
            cursor.close()
        else:
            print('Matching Failed For Death Name...of file: '+ str(filename))
            log_into_text_file('Matching Failed For Death Name...of file: '+ str(filename))
        endtime=datetime.now()
        difference = endtime - starttime
        print('End Processing Data no: '+str(ind+1)+'  In '+calculate_hours_minutes(difference)+' of file: '+ str(filename))
        log_into_text_file('End Processing Data no: '+str(ind+1)+'  In '+calculate_hours_minutes(difference)+' of file: '+ str(filename))
        # print('Generating Result For Name: '+str(dfDeathData['DEATH_DECEASED_NAME'][ind])+' And Father Name: '+dfDeathData['DEATH_FATHER_NAME'][ind]+'.. \n')
        # dfResult=dfRCData.loc[((dfRCData['death_name_matching_score'] >= 90) & (dfRCData['death_father_name_matching_score'] >= 90))]
        # print('We Have :'+str(len(dfResult.index))+' Matching Rows For Name: '+str(dfDeathData['DEATH_DECEASED_NAME'][ind])+' And Father Name: '+str(dfDeathData['DEATH_FATHER_NAME'][ind])+'.. \n')

def read_and_process_csv_for_name_and_fathername(deathdatafilepath):
    print("start Processing of death data file:"+str(deathdatafilepath))
    log_into_text_file("start Processing of death data file:"+str(deathdatafilepath))
    # Loading Death data from csv to dataframe
    dfDeathData=pd.read_csv(deathdatafilepath)
    
    #Cleaning Name And Father Name
    # dfDeathData=dfDeathData.dropna(subset=['DEATH_DECEASED_NAME']) 
    # dfDeathData=dfDeathData.dropna(subset=['DEATH_FATHER_NAME']) 
    dfDeathData["cleanDeathName"]=cleanName(dfDeathData["DEATH_DECEASED_NAME"])
    dfDeathData["cleanDeathFatherName"]=cleanName(dfDeathData["DEATH_FATHER_NAME"])

    # break Death data into two sets 
    # 1. That have Father name is not '' 
    # 2. That have father name is ''

    dfDeathdata_index = dfDeathData[dfDeathData['cleanDeathFatherName'] == '' ].index
    dfDeathData_NameOnly=dfDeathData[dfDeathData['cleanDeathFatherName'] == '' ]
    dfDeathData.drop(dfDeathdata_index, inplace = True)

    RCDeatilsFileList = glob.glob(os.path.join("tbl_D_*.csv")) 
    for rcfile in RCDeatilsFileList:
        print("Start Processing of Death Data file of path: "+str(deathdatafilepath)+" Against RC Data Of : "+ str(rcfile))
        log_into_text_file("Start Processing of Death Data file of path: "+str(deathdatafilepath)+" Against RC Data Of : "+ str(rcfile))
        # Load RC Details Data
        dfRCdata=pd.read_csv(rcfile) #,skiprows=range(1, 221701),nrows=100

        #Cleaning Name And Father Name
        # dfRCdata=dfRCdata.dropna(subset=['NAME']) 
        # dfRCdata=dfRCdata.dropna(subset=['FATHERNAME']) 
        dfRCdata["cleanRCName"]=cleanName(dfRCdata["NAME"])
        dfRCdata["cleanRCFatherName"]=cleanName(dfRCdata["FATHERNAME"])

        # Matched Name and Then Father Name
        matchesName = fpd.fuzzy_merge(dfDeathData, dfRCdata,
                                left_on=['cleanDeathName'],
                                right_on=['cleanRCName'],
                                keep_left=['death_main_id','DEATH_DECEASED_NAME', 'DEATH_FATHER_NAME','death_address','cleanDeathName','cleanDeathFatherName'],
                                keep_right=['PERMANENT_RCNO','NAME','FATHERNAME','BlockMunicipalityCode','RCCATEGORY','cleanRCName','cleanRCFatherName'],
                                ignore_case=True,
                                method='levenshtein',
                                threshold =0.9                          
                                )

        # Cleanning RC Fathername of matchesName
        matchesName=matchesName.dropna(subset=['cleanRCFatherName']) 

        # Findings On Levenshtein Method first Name then Fathername over Reulted set
        matchesNameFatherName = fpd.fuzzy_merge(dfDeathData, matchesName,
                                left_on=['cleanDeathFatherName'],
                                right_on=['cleanRCFatherName'],
                                keep_left=['death_main_id','DEATH_DECEASED_NAME', 'DEATH_FATHER_NAME','death_address','cleanDeathName','cleanDeathFatherName'],
                                keep_right=['PERMANENT_RCNO','NAME','FATHERNAME','BlockMunicipalityCode','RCCATEGORY','cleanRCName','cleanRCFatherName'],
                                ignore_case=True,
                                method='levenshtein',
                                threshold =0.9                          
                                )
        # Insert name and fathername matches to the RC database
        # print(matchesNameFatherName)

        if len(matchesNameFatherName.index)>0:
            print('We Have :'+str(len(matchesNameFatherName.index))+' Matching Rows For Names and Father Names of file: '+ str(deathdatafilepath))
            log_into_text_file('We Have :'+str(len(matchesNameFatherName.index))+' Matching Rows For Names and Father Names of file: '+ str(deathdatafilepath))
            # print('Generating CSV File For Result Of '+str(len(dfRCData_level_2_filter.index))+' Rows\n')
            cursor = cnxn.cursor()
            # Insert Dataframe into SQL Server:
            for index, row in matchesNameFatherName.iterrows():
                retry_flag = True
                retry_count = 0
                while retry_flag and retry_count < 5:
                    try:
                        cursor.execute("INSERT INTO tbl_death_matching_name_father_name(death_main_id,rc_no,rc_name,rc_father_name,rc_blockmunicipalitycode,rc_category) values(?,?,?,?,?,?)", int(row.death_main_id), str(row.PERMANENT_RCNO), str(row.NAME),str(row.FATHERNAME),str(row.BlockMunicipalityCode),str(row.RCCATEGORY))
                        cnxn.commit()
                        retry_flag = False
                    except:
                        print ("Retry after 1 sec:From Matching Name and FatherName")
                        retry_count = retry_count + 1
                        time.sleep(1)
            cursor.close()
        else:
            print('Matching Failed For Name and Father Name of file: '+ str(deathdatafilepath))
            log_into_text_file('Matching Failed For Name and Father Name of file: '+ str(deathdatafilepath))

        # Matched Only For Name 
        matchesName = fpd.fuzzy_merge(dfDeathData_NameOnly, dfRCdata,
                                left_on=['cleanDeathName'],
                                right_on=['cleanRCName'],
                                keep_left=['death_main_id','DEATH_DECEASED_NAME', 'DEATH_FATHER_NAME','death_address','cleanDeathName','cleanDeathFatherName'],
                                keep_right=['PERMANENT_RCNO','NAME','FATHERNAME','BlockMunicipalityCode','RCCATEGORY','cleanRCName','cleanRCFatherName'],
                                ignore_case=True,
                                method='levenshtein',
                                threshold =1.0                         
                                )
        # Insert name and fathername matches to the RC database
        # print(matchesName)

        if len(matchesName.index)>0:
            print('We Have :'+str(len(matchesName.index))+' Matching Rows For Names of file: '+ str(deathdatafilepath))
            log_into_text_file('We Have :'+str(len(matchesName.index))+' Matching Rows For Names of file: '+ str(deathdatafilepath))
            # print('Generating CSV File For Result Of '+str(len(dfRCData_level_2_filter.index))+' Rows\n')
            cursor = cnxn.cursor()
            # Insert Dataframe into SQL Server:
            for index, row in matchesName.iterrows():
                retry_flag = True
                retry_count = 0
                while retry_flag and retry_count < 5:
                    try:
                        cursor.execute("INSERT INTO tbl_death_matching_name_father_name(death_main_id,rc_no,rc_name,rc_father_name,rc_blockmunicipalitycode,rc_category) values(?,?,?,?,?,?)", int(row.death_main_id), str(row.PERMANENT_RCNO), str(row.NAME),str(row.FATHERNAME),str(row.BlockMunicipalityCode),str(row.RCCATEGORY))
                        cnxn.commit()
                        retry_flag = False
                    except:
                        print("Retry after 1 sec: From Matching Only Name")
                        retry_count = retry_count + 1
                        time.sleep(1)
            cursor.close()
        else:
            print('Matching Failed For Name of file: '+ str(deathdatafilepath))
            log_into_text_file('Matching Failed For Name of file: '+ str(deathdatafilepath))
        print("End Processing of Death Data file of path: "+str(deathdatafilepath)+" Against RC Data Of : "+ str(rcfile))
        log_into_text_file("End Processing of Death Data file of path: "+str(deathdatafilepath)+" Against RC Data Of : "+ str(rcfile))



# def match_execute():
#     log_into_text_file("Started Processing For matching....")
#     RCDeatilsFileList = glob.glob(os.path.join("tbl_D_*.csv"))    
#     deathFilesList = os.listdir("./CSV_FROM_SERVER") 
#     number_of_elements = len(deathFilesList)        
#     current_directory = os.getcwd()
#     PathToGetDeathFiles=Path(os.path.join(current_directory, "CSV_FROM_SERVER"))    
#     for deathfile in deathFilesList:
#             read_and_process_csv_for_block(deathfile)
#             read_and_process_csv_for_pin(deathfile)
#             read_and_process_csv_for_name_and_fathername(str(PathToGetDeathFiles.joinpath(deathfile)))
#             os.remove(deathfile)
            
#     log_into_text_file("End Of Processing For matching....")


# if __name__ == '__main__':
#     log_into_text_file("Started Processing For matching....")
#     RCDeatilsFileList = glob.glob(os.path.join("tbl_D_*.csv"))    
#     deathFilesList = os.listdir("./CSV_FROM_SERVER") 
#     number_of_elements = len(deathFilesList)
        
#     current_directory = os.getcwd()
#     PathToGetDeathFiles=Path(os.path.join(current_directory, "CSV_FROM_SERVER"))

#     for deathfile in deathFilesList:
#         read_and_process_csv_for_block(deathfile)
#         read_and_process_csv_for_pin(deathfile)
#         read_and_process_csv_for_name_and_fathername(str(PathToGetDeathFiles.joinpath(deathfile)))
#         os.remove(deathfile)
            
#     log_into_text_file("End Of Processing For matching....")

            

#     # with Pool(number_of_elements) as p1:
#     #     p1.map(read_and_process_csv_for_block, dirList)
    
#     # with Pool(number_of_elements) as p2:
#     #     p2.map(read_and_process_csv_for_pin, dirList)
        
#     # with Pool(len(list)) as p:        
#     #     p.starmap(read_and_process_csv_for_name_and_fathername, list)
