# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import argparse
import pandas as pd
import xnat
#import pydicom
import sys
#from datetime import datetime
import os
#import subprocess
#
from downloadtools.utils import dbreader
#from downloadtools import dbsearch
from downloadtools.utils import download_dcm
from downloadtools.utils import checkdatabase
from downloadtools.utils import anonymize
from downloadtools.utils import convert2nii
from downloadtools.utils import download_dcm_noid
from downloadtools.utils import download_dcmname
#%%
def main():
    
       
    if hasattr(sys, "ps1"):
        project = "01-M-0192"
        dosnapshot = True
        #samplesubj = "7832795"
        sdanid = ["24584"]
        sdanid = False
        date = "10-04-2022"
        download = True
        SeriesName = [""]
        unzip = True
        keepdicom = True
        downloaddir = "/home/zugmana2/Desktop/testsnap"
    else :
        parser = argparse.ArgumentParser(description="Download data from XNAT. created by Andre Zugman")
        parser.add_argument('-i', '--id', nargs='+',dest="id", action='store', type=str, required=False, help='id of subject ')
        parser.set_defaults(id=False)
        parser.add_argument('-o','--output', action='store', type=str, required=True, help='output path.')
        parser.add_argument('-d', '--date',action='store', dest="date", type=str, required=False, help='Session Date in mm-dd-yyyy (i.e.: 12-30-2022')        
        parser.set_defaults(date="")
        parser.add_argument('--dosnapshot', action='store_true',dest='dosnapshot', help='save a table with info of data stored in the project.')
        parser.set_defaults(dosnapshot=False)
        parser.add_argument('-s', '--series', nargs='+', action='store', dest='SeriesName', type=str, help='Series Name')
        parser.add_argument('-p', '--project', action='store', dest='project', type=str, help='project name - as defined in xnat')
        parser.add_argument('--keepdicom', action='store_true',dest='keepdicom', help='Keep the dicoms. This will not run dcm2niix and keep anonymized dicoms.')
        parser.set_defaults(keepdicom = False)
        #parser.add_argument('--partial', action='store_true',dest='corr_type' , help='use partial correlations')
        parser.set_defaults(SeriesName=[""])
        parser.set_defaults(project="01-M-0192")
        args = parser.parse_args()
        #print(parser.print_help())
        #set variables for ease
        project = args.project
        dosnapshot = args.dosnapshot
        #samplesubj = "7832795"
        sdanid = args.id
        date = args.date
        download = True
        SeriesName = args.SeriesName
        keepdicom = args.keepdicom
        unzip = True
        downloaddir = args.output
    if not os.path.exists(os.path.join("/home",os.environ["USER"],".netrc")) :
        sys.exit("please configure .netrc file")
    with xnat.connect("https://fmrif-xnat.nimh.nih.gov") as xsession :
        if dosnapshot :
            dbsearched = dbreader(0)
            dbsearched["subjects"] = dbsearched.loc[:,1].str.replace("-","")
            dbsearched["subjects"] = dbsearched.loc[:,"subjects"].str.replace(r"[a-z]+","", regex=True)
            dbsearched["subjects"] = dbsearched.loc[:,"subjects"].str.replace(r"[A-Z]+","", regex=True)
            dbsearched["subjects"] = dbsearched.loc[:,"subjects"].str.replace(",","")
            dbsearched["subjects"] = pd.to_numeric(dbsearched["subjects"], errors = "coerce")
            dbsnapshot = checkdatabase(xsession, project)
            dbsnapshot["subjects"] = dbsnapshot["subjects"].astype(float)
            dbsnapshot = dbsnapshot.merge(dbsearched,on= "subjects")
            dbsnapshot = dbsnapshot.rename(columns={0: "sdanid", 1: "MRN", 2: "DOB",3: "Last Name", 4: "First Name" })
            dbsnapshot.drop(["subjects",5,6,7], axis = 1, inplace = True)
            dbsnapshot = dbsnapshot.reindex(columns= ['sdanid', 'MRN',"AccessionNumber",
                   'DOB', 'Last Name', 'First Name','seriesName', 'uri', 'date-series', 'date-session'])
            os.makedirs(os.path.join(downloaddir),  exist_ok = True) 
            dbsnapshot.to_csv(os.path.join(downloaddir,"dbsnapshot.csv"),index = False)
    
            download = False   
        if download :
            if sdanid :
                for i in sdanid :
            
                    #print (i)
                    dbsearched = dbreader(i)
                    MRN = dbsearched.loc[0,1]
                    MRN = MRN.replace("-","")
                    LastName = dbsearched.loc[0,3]
                    LastName = LastName.replace(",","")
                    FirstName = dbsearched.loc[0,4]
                    
                    try: 
                        download_dcm(xsession, project, MRN, i, date, SeriesName, downloaddir, unzip )
                        if keepdicom :
                            anonymize(downloaddir, i)
                        else :
                            convert2nii(downloaddir, i)
                    except :
                        #print ("Failed to download data as specified. Does subject exist in the database?")
                        download_dcmname(xsession, project, FirstName, LastName, i, date, SeriesName, downloaddir, unzip)
                        if keepdicom :
                            anonymize(downloaddir, i)
                        else :
                            convert2nii(downloaddir, i)
                            #sys.exit("Failed to download data as specified. Does subject exist in the database?")
            if not sdanid :
                print ("no id provided - looking for date")
                print ("this can take longer. Please wait")
                sdanid = download_dcm_noid( xsession, project, [date], SeriesName, downloaddir, unzip)
                for i in sdanid :
                    if keepdicom :
                        anonymize(downloaddir, i)
                    else :
                        convert2nii(downloaddir, i)
if __name__ == '__main__':
    main()
