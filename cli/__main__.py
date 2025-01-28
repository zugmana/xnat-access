#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 14 13:46:33 2024

@author: zugmana2
"""

import argparse
import pandas as pd
import sys
import os
import getpass
import tempfile
import warnings
import requests
import time
#
if not hasattr(sys, "ps1"):
    from . __version__ import __version__
from downloadtools.restutilities import listsubjects, listsession, listscans, queryxnatID, namecheck,downloadfile ,tupletodownload,read_config_connect
from downloadtools.restutilities import getphysioforsesseions
from downloadtools.pgsqlutils import checkrobin
from downloadtools.utils import simplifystring, unzip_and_sort,convert2nii,anonymize,makebids,convert2nii2

import multiprocessing
#from downloadtools import dbsearch


def main():
    
       
    if hasattr(sys, "ps1"):
        project = "01-M-0192"
        dosnapshot = False
        sdanid = ["24851","24851","24851","24851"]#["23262","23298"]#["24624","24733"]
        #sdanid = False
        date = ["05/20/2024", "06/02/2024", "08/10/2024", "09/07/2024"]#
        download = True
        SeriesName = [""]
        unzip = True
        keepdicom = False
        search_name = False
        downloaddir = "/EDB/SDAN/temp/test07-29"
        user = None
        password = None
        MRNid = None 
        dosnapshotsubject = False
        search_robin = True
        dobids = True
        physio = True
        nworkers = 4
        xnaturl = "https://fmrif-xnat.nimh.nih.gov"
        #os.environ["TMP"] = "/EDB/SDAN/temp/"
    else :
        parser = argparse.ArgumentParser(description="Download data from XNAT v {}. Created by Andre Zugman".format(__version__))
        parser.add_argument('-v', '--version', action='version',
                    version='version: {}'.format(__version__))
        parser.add_argument('-i', '--id', nargs='+',dest="id", action='store', type=str, required=False,
                            help='id of subject or list of subjects')
        parser.set_defaults(id=None)
        parser.add_argument('-o','--output', action='store', type=str, required=True,
                            help='output path.')
        parser.add_argument('-d', '--date', nargs='+',action='store', dest="date", type=str, required=False,
                            help='Session Date in mm-dd-yyyy (i.e.: 12-30-2022).')        
        parser.set_defaults(date=[""])
        parser.add_argument('--dosnapshot', action='store',dest='dosnapshot',
                            help='save a table with info of data stored in the project. Choose level: project,session,scans. Scans will take longer')
        parser.set_defaults(dosnapshot=False)
        parser.add_argument('-s', '--series', nargs='+', action='store', dest='SeriesName', type=str,
                            help='Series Name')
        parser.add_argument('-p', '--project', action='store', dest='project', type=str,
                            help='project name - as defined in xnat')
        parser.add_argument('--keepdicom', action='store_true',dest='keepdicom',
                            help='Keep the dicoms. This will not run dcm2niix and keep anonymized dicoms.')
        parser.set_defaults(keepdicom = False)
        parser.add_argument('--search_name', action='store_true',dest='search_name',
                            help='Search XNAT by name and not MRN. Default is to search by MRN, and if MRN not found search for Name')
        parser.set_defaults(search_name = False)
        parser.add_argument('--dobids', action='store_true',dest='dobids',
                            help='Export a BIDS directory.')
        parser.set_defaults(dobids = False)
        parser.add_argument('--physio', action='store_true',dest='physio',
                            help='get physio data. Not really working in this version. But physio data is often missing')
        parser.set_defaults(physio = False)
        parser.add_argument('--not-robin', nargs='+', type=str, action='store',dest='MRNid',
                            help="""Do not use robin id. In this case provide the MRN manually with no "-".
                            You can also use this with other ID (i.e.: NDAR GUID). In this case the data will use the id provided.
                            Only use this if you are sure it is safe to do so.""")
        parser.add_argument('--nworkers', type=int,dest='nworkers',
                            help="""Number of workers. If set to 1 will follow old behavior,
                            if set to > 1 it will allow for multiple downloads and unzipping in parallel.
                            It may not necesserally speed up things for you.""")
        parser.set_defaults(nworkers = 2)
        parser.set_defaults(MRNid = None)
        parser.set_defaults(SeriesName=[""])
        parser.set_defaults(project="01-M-0192")
        args = parser.parse_args()
        nworkers = args.nworkers
        project = args.project
        dosnapshot = args.dosnapshot
        sdanid = args.id
        date = args.date
        download = True
        SeriesName = args.SeriesName
        keepdicom = args.keepdicom
        search_name = args.search_name
        unzip = True
        downloaddir = args.output
        user = None
        password = None
        dosnapshotsubject = False
        dobids = args.dobids
        MRNid = args.MRNid
        physio = args.physio
        xnaturl = "https://fmrif-xnat.nimh.nih.gov"
    #just setting some options
    if physio :
        print("\n")
        print("############################")
        print("###########WARNING##########")
        print("The data inside the physio subdirectory in nifti will not be anonymized")
        print("Please inspect your data carefully for any PII")
        print("############################")
        print("\n")
    #if dosnapshot and  isinstance(sdanid, list):
    #    dosnapshotsubject = True
    #    download = False
    #    dosnapshot = False
    if keepdicom :
        dobids = False
    if MRNid is None :
        search_robin = True
    else :
        search_robin = False
    if search_name:
        search_robin = False
    if sdanid is not None :
        if os.path.isfile(sdanid[0]):
            sdanid = pd.read_csv(sdanid[0],header = None, index_col= None)
            
            sdanid = sdanid.iloc[:,0].to_list()
            print(sdanid)
        else :
            sdanid = " ".join(sdanid).split(" ")
            while "" in sdanid:
                sdanid.remove("")
            while " " in sdanid:
                sdanid.remove(" ")
        sdanid = [x[1:] if x.startswith('s') else x for x in sdanid if x.isdigit() or x.startswith('s')]


    else :
        sdanid = False
    if MRNid is not None:
        MRNid = " ".join(MRNid).split(" ")
        while "" in sdanid:
            MRNid.remove("")
        while " " in sdanid:
            MRNid.remove(" ")
    if all([x=="" for x in date]):
        
        if sdanid != False and len(sdanid) != len(date):
            date = [""]*len(sdanid)
    else :
        date = " ".join(date).split(" ")
        while date[-1] == "" :
            date = date[:-1]
        while date[-1] == " ":
            date = date[:-1]
        if sdanid != False and len(sdanid) != len(date):
            print("Number of dates and id is different. Will search without date for the last ids")
            diffdates = len(sdanid) - len(date)
            if diffdates >= 1:
                date = date + [""]*diffdates
            elif diffdates < 0: 
                sys.exit("You provided more dates than subjects. Double check what you are doing")
    SeriesName = [simplifystring(x) for x in SeriesName]
    saveshot = False
    #End of setting options
    # Getuser and password
    user = getpass.getuser()
    #user = getpass.getpass(prompt="Please enter username : ")
    print ("current user is {}".format(user))
    password = getpass.getpass(prompt="Please enter Password : ")
    if not (os.getenv("TMPDIR") or os.getenv("TEMP") or os.getenv("TMP")) :
        warnings.warn("WARNING : tempfile not specified by user. Please consider setting your TMP path before running this script" )
        print("Please type : export TMP=/home/{}/tmp or some other approapriate path.".format(getpass.getuser()))
        print("using system default may cause problems")
        print("Will try /home/{}/tmp".format(getpass.getuser()))
        tempfile.tempdir = "/home/{}/tmp".format(getpass.getuser())
        if not os.path.isdir("/home/{}/tmp".format(getpass.getuser())) :
            os.makedirs("/home/{}/tmp".format(getpass.getuser()))
    with requests.session() as connect:
        
        connect.auth = (user, password)
        connect.xnaturl = xnaturl
        connect.project = project
        connect.base_url = f'{xnaturl}/data/projects/{project}'
        # test connection
        response = connect.get(connect.base_url)
        if not response.ok:
            print(f"You can't access XNAT project {project} with the credentials provided. Will try alternate method")

            # Fallback on alternative auth method
            payload = read_config_connect()
            payload["username"] = user
            payload["password"] = password
            
            with requests.Session() as fallback_connect:
                response = fallback_connect.post(f"{xnaturl}/login", data=payload)
                
                if response.ok:
                    # Copy cookies from fallback_connect to connect
                    connect.cookies.update(fallback_connect.cookies)
                    connect.auth = None
                    
                    response = connect.get(connect.base_url)
                    
                    if response.ok:
                        print(f"Successfully accessed XNAT project {project} with alternative method.")
            
                    if not response.ok:
                        warnings.warn(f"You can't access xnat project {project} with the credenctials provided.")
                        sys.exit("Ending program")
        if dosnapshot == "project":
            print(f"YOUR OUTPUT WILL BE IN:{downloaddir}/dbsnapshot.csv")
            dbsnapshot = pd.DataFrame()
            allsubj = listsubjects(connect)
            dbsearched = checkrobin(20000000,allsubj=True)
            allsubj = allsubj.rename(columns={'label': 'mrn'})
            allsubj["mrn"] = pd.to_numeric(allsubj["mrn"],errors = "coerce", downcast="integer")
            allsubj.dropna(subset=["mrn"],inplace=True)
            dbsearched["mrn"] = pd.to_numeric(dbsearched["mrn"],errors = "coerce", downcast="integer")
            dbsearched.dropna(subset=["mrn"],inplace=True)
            dbsnapshot = allsubj.merge(dbsearched,on= "mrn",how="inner")
            dbsnapshot["mrn"] = dbsnapshot["mrn"].apply(lambda x: str(int(x)))
            
    
            download = False
            saveshot = True
        if dosnapshot == "session":
            print(f"YOUR OUTPUT WILL BE IN:{downloaddir}/dbsnapshot.csv")
            dbsnapshot = pd.DataFrame()
            #headerinfo = pd.DataFrame()
            if sdanid:
                for idd,i in enumerate(sdanid):
                    dbsearched = checkrobin(i)
                    if len(dbsearched) == 1:
                        MRN = dbsearched.loc[0,"mrn"]
                        if not MRN:
                            warnings.warn(f"Oh No! Couldn't find MRN for {i}. Check the database")
                            continue
                    else:
                        print(f"something went wrong when checking database for this subject {i}. please check robin")
                        continue
                    xnatID = queryxnatID(MRN,connect)
                    # if not xnatID:
                    #     continue
                    xnatID = xnatID.loc[0,"ID"]
                    sessions = listsession(connect,xnatID,date=date[idd])
                    sessions = sessions.rename(columns={'subject_label': 'mrn'})
                    sessions["mrn"] = pd.to_numeric(sessions["mrn"],errors = "coerce", downcast="integer")
                    sessions.dropna(subset=["mrn"],inplace=True)
                    dbsearched["mrn"] = pd.to_numeric(dbsearched["mrn"],errors = "coerce", downcast="integer")
                    dbsearched.dropna(subset=["mrn"],inplace=True)
                    dbsnapshot = sessions.merge(dbsearched,on= "mrn",how="inner")
                    dbsnapshot["mrn"] = dbsnapshot["mrn"].apply(lambda x: str(int(x)))
                    
                    
            else :
                allsessions = listsession(connect)
                dbsearched = checkrobin(20000000,allsubj=True)
                allsessions = allsessions.rename(columns={'subject_label': 'mrn'})
                allsessions["mrn"] = pd.to_numeric(allsessions["mrn"],errors = "coerce", downcast="integer")
                allsessions.dropna(subset=["mrn"],inplace=True)
                dbsearched["mrn"] = pd.to_numeric(dbsearched["mrn"],errors = "coerce", downcast="integer")
                dbsearched.dropna(subset=["mrn"],inplace=True)
                dbsnapshot = allsessions.merge(dbsearched,on= "mrn",how="inner")
                dbsnapshot["mrn"] = dbsnapshot["mrn"].apply(lambda x: str(int(x)))
            download = False
            saveshot = True    
            
        if dosnapshot == "scans":
            print(f"YOUR OUTPUT WILL BE IN:{downloaddir}/dbsnapshot.csv")
            #print(f"getting a list of scans for subject {}")
            #allsessions = listsession(connect)
            dbsnapshot = pd.DataFrame()
            headerinfo = pd.DataFrame()
            if sdanid:
                for idd,i in enumerate(sdanid):
                    if search_robin :
                        dbsearched = checkrobin(i)
                        if len(dbsearched) == 1:
                            MRN = dbsearched.loc[0,"mrn"]
                            if not MRN:
                                warnings.warn(f"Oh No! Couldn't find MRN for {i}. Check the database")
                                continue
                        else:
                            print(f"something went wrong when checking database for this subject {i}. please check robin")
                            continue
                    else:
                        MRN = MRNid[idd]
                        dbsearched = []
                    xnatID = queryxnatID(MRN,connect)
                    if not isinstance(xnatID, pd.DataFrame):
                         continue
                    xnatID = xnatID.loc[0,"ID"]
                    sessions = listsession(connect,xnatID,date=date[idd])
                    scans,hdr = listscans(connect,sessions,get_header=True)
                    scans["SDANID"] = i
                    namesdicom = hdr["Patient&rsquo;s Name"].apply(simplifystring).unique()
                    namesdicom = list(namesdicom)
                    if len(namesdicom) > 1:
                        print(f"ooops. more than one name in the dicoms of {i}: {namesdicom}")
                        continue
                    if search_robin:
                        namecheck(dbsearched,namesdicom[0])
                    scans["sdanid"] = i
                    dbsnapshot = pd.concat([dbsnapshot,scans])
                    #headerinfo = pd.concat([headerinfo,hdr])
            else :
               print(f"YOUR OUTPUT WILL BE IN:{downloaddir}/dbsnapshot.csv") 
               print("This will download data for everyone. It will take a long time... Will not perform dicom dump")
               dbsearched = checkrobin(20000000,allsubj=True)
               for i in dbsearched["sdan_id"].to_list():
                   dbsearchedind = checkrobin(i)
                   if len(dbsearchedind) == 1:
                       MRN = dbsearchedind.loc[0,"mrn"]
                       if not MRN:
                           warnings.warn(f"Oh No! Couldn't find MRN for {i}. Check the database")
                           continue
                   else:
                       print(f"something went wrong when checking database for this subject {i}. please check robin")
                       continue
                   xnatID = queryxnatID(MRN,connect)
                   if not isinstance(xnatID, pd.DataFrame):
                        continue
                   xnatID = xnatID.loc[0,"ID"]
                   sessions = listsession(connect,xnatID)
                   scans,hdr = listscans(connect,sessions,get_header=False)
                   scans["SDANID"] = i
                   dbsnapshot = pd.concat([dbsnapshot,scans])
                   headerinfo = pd.concat([headerinfo,hdr])
                   if len(hdr) == 0:
                       continue
                   #print(hdr.columns)
                   namesdicom = hdr["Patient&rsquo;s Name"].apply(simplifystring).unique()
                   namesdicom = list(namesdicom)
                   if len(namesdicom) > 1:
                       print(f"ooops. more than one name in the dicoms of {i}: {namesdicom}")
                       continue
                   
            download = False
            saveshot = True
        if dosnapshot :
            if dosnapshot not in ["scans","session","project"]:
                
                print("This dbsnapshot input is not valid.")
                sys.exit()
        if saveshot:
            if os.path.isfile(os.path.join(downloaddir,"dbsnapshot.csv")):
                dbsnapshot.to_csv(os.path.join(downloaddir,"dbsnapshot.csv"),index = False, header =False, mode='a')
            else :
                os.makedirs(downloaddir,exist_ok = True)
                dbsnapshot.to_csv(os.path.join(downloaddir,"dbsnapshot.csv"),index=False)
                   
        if download == True:
            os.makedirs(os.path.join(downloaddir),  exist_ok = True)
            with tempfile.TemporaryDirectory(suffix=None, prefix=None) as tempdir :
                downloadlist = []
                unzipargs = []
                anonymizeargs = []
                niftiargs = []
                if sdanid:
                    
                    for idd,i in enumerate(sdanid):
                        if search_robin :
                            dbsearched = checkrobin(i)
                            if len(dbsearched) == 1:
                                MRN = dbsearched.loc[0,"mrn"]
                                if not MRN:
                                    warnings.warn(f"Oh No! Couldn't find MRN for {i}. Check the database")
                                    continue
                            else:
                                print(f"something went wrong when checking database for this subject {i}. please check robin")
                                continue
                        else:
                            MRN = MRNid[idd]
                        #print(MRN)
                        xnatID = queryxnatID(MRN,connect)
                        if not isinstance(xnatID, pd.DataFrame):
                             continue
                        #print(xnatID)
                        # if not xnatID:
                        #     continue
                        #print(i)
                        #print(date[idd])
                        xnatID = xnatID.loc[0,"ID"]
                        sessions = listsession(connect,xnatID,date=date[idd])
                        #print(sessions['xnat:mrsessiondata/date'])
                        scans,hdr = listscans(connect,sessions,get_header=True)
                        #print(scans)
                        if len(scans) == 0:
                            print("\n")
                            print("########################################################")
                            warnings.warn(f" =( Couldn't find {i} scans on date {date[idd]}. Check the database for project and date")
                            print("########################################################")
                            continue
                        scans['series_description'] = scans['series_description'].apply(simplifystring)
                        #print(scans)
                        scans = scans[scans['series_description'].str.contains('|'.join(SeriesName))]
                        #print(scans)
                        
                        #Get session cookies
                        
                        # Create list of downloads commands
                        #baseurl,fileuri,destination,cookies
                        response = connect.get(connect.base_url)# This should allow the seesion to be handled by requests and not die
                        #time.sleep(30)
                        if response.ok:
                            cookies = requests.utils.dict_from_cookiejar(connect.cookies)
                        while not response.ok : # Try restarting connect and get cookies
                            connect.auth = (user, password)
                            connect.xnaturl = xnaturl
                            connect.project = project
                            connect.base_url = f'{xnaturl}/data/projects/{project}'
                            # test connection
                            response = connect.get(connect.base_url)
                            #time.sleep(30)
                            cookies = requests.utils.dict_from_cookiejar(connect.cookies)
                            
                        for _,j in scans.loc[scans["xsiType"] == "xnat:mrScanData"].iterrows():
                            downloadpath = os.path.join(tempdir,"DICOM",
                                                        f"sub-{i}",
                                                        j["date"],
                                                        f"{j['series_description']}_{j['ID']}.zip")
                            unzippath = os.path.join(tempdir,"DICOM",
                                                        f"sub-{i}",
                                                        j["date"],
                                                        f"{j['series_description']}_{j['ID']}")
                            dicomorigpaths = os.path.join(tempdir,"DICOM")
                            dicompaths = os.path.join(downloaddir,"DICOM")
                            downloadlist.append((connect.xnaturl,f'{j["URI"]}/resources/DICOM/files',downloadpath,cookies))
                            #print((connect.xnaturl,f'{j["URI"]}/resources/DICOM/files',downloadpath,cookies))
                            unzipargs.append((downloadpath,unzippath))
                            anonymizeargs.append((dicomorigpaths,dicompaths,i))
                        if physio :
                            # get physio list
                            # for each session:
                            result,datahdr = getphysioforsesseions(connect,sessions)
                            if len(datahdr) > 0:
                                
                                for file,filedata in datahdr.iterrows() :
                                    #Obs: will use downloaddir since the files are small and don't require any added processing.
                                    downloadpath = os.path.join(downloaddir,"nifti",
                                                            f"sub-{i}",
                                                            j["date"],"physio",f"{filedata['Name']}")
                                    
                                    downloadlist.append((connect.xnaturl,f'{filedata["URI"]}',downloadpath,cookies,False))
                                    print((connect.xnaturl,f'{filedata["URI"]}',downloadpath,cookies,False))
                        niftiargs.append((os.path.join(tempdir,"DICOM"),os.path.join(downloaddir,"nifti"),i)) # This should be one per subject
                        print("downloading images - please wait")
                    
                    with multiprocessing.Pool(processes=nworkers) as pool:
                        pool.starmap(downloadfile, downloadlist)
                        pool.map(unzip_and_sort, unzipargs)
                    if keepdicom :
                        #downloaddirlocal = os.path.join(downloaddir,"DICOM")
                        with multiprocessing.Pool(processes=nworkers) as pool:
                            pool.starmap(anonymize, anonymizeargs)
                        #anonymize(tempdir,downloaddirlocal, i)
                    else :
                        print("convert to nii")
                        with multiprocessing.Pool(processes=nworkers) as pool:
                            pool.starmap(convert2nii2, niftiargs)
                        # downloaddirlocal = os.path.join(downloaddir,"nifti")
                        # os.makedirs(downloaddirlocal,exist_ok=True)
                        # convert2nii(os.path.join(tempdir,"DICOM"),downloaddirlocal, i)
                        
                    if dobids :
                        downloaddirlocal = os.path.join(downloaddir,"nifti")
                        makebids(downloaddirlocal,tempdir, True)
                else :
                    print("download with no sdanid not yet implemented")
                    sys.exit()
            #subjsees = listsession(connect,)
            # dbsearched = checkrobin(20000000,allsubj=True)
            # allsessions = allsessions.rename(columns={'subject_label': 'mrn'})
            # allsessions["mrn"] = pd.to_numeric(allsessions["mrn"],errors = "coerce", downcast="integer")
            # allsessions.dropna(subset=["mrn"],inplace=True)
            # dbsearched["mrn"] = pd.to_numeric(dbsearched["mrn"],errors = "coerce", downcast="integer")
            # dbsearched.dropna(subset=["mrn"],inplace=True)
            # dbsnapshot = allsessions.merge(dbsearched,on= "mrn",how="inner")
            # dbsnapshot["mrn"] = dbsnapshot["mrn"].apply(lambda x: str(int(x)))
            # os.makedirs(os.path.join(downloaddir),  exist_ok = True)
            # dbsnapshot.to_csv(os.path.join(downloaddir,"session-dbsnapshot.csv"),index = False)
            # download = False    
    # Check if SSL_CERT_DIR is set
        # if not os.getenv("SSL_CERT_DIR"):
        #     print("SSL_CERT_DIR not setup")
        #     os.environ["SSL_CERT_DIR"] = "/etc/pki/NIH/"
        #     os.environ["REQUESTS_CA_BUNDLE"] = "/etc/pki/NIH/"
if __name__ == '__main__':
    main()    