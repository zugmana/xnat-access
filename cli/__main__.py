# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import argparse
#import pandas as pd
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
#%%
def main():
    with xnat.connect("https://fmrif-xnat.nimh.nih.gov") as xsession :
       
        if hasattr(sys, "ps1"):
            project = "01-M-0192"
            dosnapshot = False
            #samplesubj = "7832795"
            sdanid = ["23799"]
            date = ""
            download = True
            SeriesName = ""
            unzip = True
            keepdicom = False
            downloaddir = "/home/zugmana2/Desktop/test"
        else :
            parser = argparse.ArgumentParser(description="download from XNAT.")
            parser.add_argument('-i', '--id', nargs='+',action='store', type=str, required=False, help='id of subject ')
            parser.add_argument('-o','--output', action='store', type=str, required=True, help='output path.')
            parser.add_argument('-d', '--date',action='store', dest="date", type=str, required=False, help='Session Date in %m-%d-%Y')        
            parser.set_defaults(date="")
            parser.add_argument('--dosnapshot', action='store_true',dest='dosnapshot', help='save a table with info of data stored in the project.')
            parser.set_defaults(dosnapshot=False)
            parser.add_argument('-s', '--series', action='store', dest='SeriesName', type=str, help='Series Name')
            parser.add_argument('-p', '--project', action='store', dest='project', type=str, help='project name - as defined in xnat')
            parser.add_argument('--keepdicom', action='store_true',dest='keepdicom', help='save a table with info of data stored in the project.')
            parser.set_defaults(keepdicom = False)
            #parser.add_argument('--partial', action='store_true',dest='corr_type' , help='use partial correlations')
            parser.set_defaults(SeriesName="")
            parser.set_defaults(project="01-M-0192")
            args = parser.parse_args()
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
            print("please configure .netrc file")
        if dosnapshot :
         
            dbsnapshot = checkdatabase(xsession, project)
            dbsnapshot.to_csv(os.path.join(downloaddir,"dbsnapshot.csv"))
            download = False   
        if sdanid :
            for i in sdanid :
        
                print (i)
                dbsearched = dbreader(i)
                MRN = dbsearched.loc[0,1]
                MRN = MRN.replace("-","")
        
        if download : 
            download_dcm(xsession, project, MRN, i, date, SeriesName, downloaddir, unzip )
            if keepdicom :
                anonymize(downloaddir, i)
            else :
                convert2nii(downloaddir)
        #%% Just some testing.
        

if __name__ == '__main__':
    main()
    