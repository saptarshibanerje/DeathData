import matching as m
import fetching as f
# from multiprocessing import Pool
import os, glob
from pathlib import Path

if __name__ == '__main__':
    # for fetching data from RC server
    f.log_into_text_file('Started Processing From File Collect Data From RC:....')
    f.get_block_muni_pin_data_from_db()
    f.get_Death_data_deatils_ChunkWise()    
    noofdist=len(f.district_list)
    for distname in f.district_list:        
        f.get_all_rc_data_as_on_date(distname)
    # with Pool(noofdist) as pdist:
    #     pdist.map(f.get_all_rc_data_as_on_date, f.district_list)
    f.log_into_text_file('End of Processing From File Collect Data From RC:....')

    # for processing data from RC server

    m.log_into_text_file("Started Processing For matching....")
    RCDeatilsFileList = glob.glob(os.path.join("tbl_D_*.csv"))    
    deathFilesList = os.listdir("./CSV_FROM_SERVER") 
    number_of_elements = len(deathFilesList)        
    current_directory = os.getcwd()
    PathToGetDeathFiles=Path(os.path.join(current_directory, "CSV_FROM_SERVER"))    
    for deathfile in deathFilesList:
        m.read_and_process_csv_for_block(deathfile)
        m.read_and_process_csv_for_pin(deathfile)
        m.read_and_process_csv_for_name_and_fathername(str(PathToGetDeathFiles.joinpath(deathfile)))
        # print(deathfile)
        os.remove("./CSV_FROM_SERVER/"+str(deathfile))
                
    m.log_into_text_file("End Of Processing For matching....")