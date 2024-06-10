#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 26 19:27:51 2022

@author: zugmana2
"""
import os
import subprocess
import pandas as pd
import pydicom
from shutil import copy2
import re
from natsort import natsorted
import multiprocessing
import xnat
from downloadtools.pgsqlutils import checkrobin2
#import dill
#import xnat

def downloadphysio(xobject,downloadpath):
    #splitpath = os.path.split(downloadpath)
    exclude_strings = ["README","upload",".html"]
    print(xobject)
    #file = splitpath[1].replace("tgz","_physio.tsv")
    physiopath = os.path.join(downloadpath,"physio")
    os.makedirs(physiopath, exist_ok = True)
    for r in xobject.resources.values() :
        print(r)
        for file in r.files.values():
            try:
                print(file.data)

                if any(series in file.data["Name"] for series in exclude_strings):
                    print("skipping README file")
                    continue
                if "ECG" in file.data["Name"] :
                    print("found physio file {}".format(file.data["Name"]))
                    file.download(os.path.join(physiopath,file.data["Name"]))
                if "Resp" in file.data["Name"] :
                    print("found physio file {}".format(file.data["Name"]))
                    file.download(os.path.join(physiopath,file.data["Name"]))
                else :
                    print("Description file {}".format(file.data["Name"]))
                    file.download(os.path.join(physiopath,file.data["Name"]))
            
            except Exception as err:
                print(err)
                print("The above exception got thrown when downloading physio data.")
                print("will attempt to use URI {}".format(file.uri))
                print("\n")
                try:
                    if any(series in file.uri for series in exclude_strings):
                        print("skipping README file")
                        continue
                    if "ECG" in file.uri :
                        print("found physio file {}".format(file.uri.split("/")[-1]))
                        file.download(os.path.join(physiopath,file.uri.split("/")[-1]))
                    if "Resp" in file.uri :
                        print("found physio file {}".format(file.uri.split("/")[-1]))
                        file.download(os.path.join(physiopath,file.uri.split("/")[-1]))
                    else :
                        print("Description file {}".format(file.uri.split("/")[-1]))
                        file.download(os.path.join(physiopath,file.uri.split("/")[-1]))
                except:
                    print("\n")
                    print("WARNING: error when downloading physio.")
                    print("\n")
                
def unzip_and_sort(args):
    downloadpath, extractpath = args
    subprocess.run(["unzip -n -j {} -d {}".format(downloadpath, extractpath)], shell=True, stdout=subprocess.DEVNULL)
    subprocess.run(["sortme {}".format(extractpath)], shell=True)               

def download_forpar(args):
    uri, path, user, password = args
    #xscan = xscandill.loads(xscandill)
    with xnat.connect("https://fmrif-xnat.nimh.nih.gov", user=user, password=password) as thissession: 
        experiment_object = thissession.create_object(uri)
        experiment_object.download(path, verbose = False)
def download_dcm(xsession, project, xmrn, sdanid, date, seriesName, downloaddir, unzip, physio, u, p, max_processes = 4) :
    xproject = xsession.projects[project]
    xnat_subject = xproject.subjects[xmrn]
    count = 0
    exclude_strings = ["REQUISITION", "SCREEN-SAVE","SAVE",".txt","README"]
    args_list = []
    downloadlist = []
    #max_processes = 4
    for session in xnat_subject.experiments.values() :
        ses_date = session.date
        #print(ses_date)
        if date in ses_date.strftime("%m-%d-%Y") : # if date undefined - check all sessions          
          for xscan in session.scans.values() :
              xsname =  xscan.series_description
              if any(series in xsname for series in exclude_strings):
                  continue
              if any(series in xsname for series in seriesName): 
                xsname = simplifystring(xsname)
                xsnumber = xscan.id
                  #if xsname not in ["REQUISITION", "SCREEN-SAVE"] :
                      #xsname = simplifystring(xsname)
                os.makedirs(os.path.join(downloaddir,"sub-{}".format(sdanid),ses_date.strftime("%m-%d-%Y")),  exist_ok = True) 
                downloadpath = os.path.join(downloaddir,"sub-{}".format(sdanid),ses_date.strftime("%m-%d-%Y"),
                                            "{}_{}.zip".format(xsname,xsnumber))
                if max_processes == 1:
                    xscan.download(downloadpath)
                count = count + 1
                uri = xscan.uri
                downloadlist.append((uri,downloadpath, u, p))
                if unzip : 
                    extractpath = os.path.join(downloaddir,"sub-{}".format(sdanid),ses_date.strftime("%m-%d-%Y"),
                                                "{}_{}".format(xsname,xsnumber))
                    args_list.append((downloadpath, extractpath))
                if max_processes == 1:
                    subprocess.run(["unzip -j {} -d {}".format(downloadpath,extractpath)], shell=True, stdout=subprocess.DEVNULL)
                    subprocess.run(["sortme {}".format(extractpath)], shell=True)
          if (count > 0) and physio :
              print("downloading physio")
              downloadpath = os.path.join(downloaddir,"sub-{}".format(sdanid),ses_date.strftime("%m-%d-%Y"))
              downloadphysio(session,downloadpath)
    #print (downloadlist)
    if max_processes > 1:
        print("downloading images - please wait")
        with multiprocessing.Pool(processes=max_processes) as pool:
            pool.map(download_forpar, downloadlist)  
        print("unzipping and checking for ME - please wait")
        with multiprocessing.Pool(processes=max_processes) as pool:
            pool.map(unzip_and_sort, args_list)  
        #clear xsession
        
    # except:
    #      print("Did not find this subject in this collection.")
    #      print("subject:{} Check if this subject is in XNAT project {}".format(sdanid, project))
            
def checkdatabase(xsession, project) :
    #count = 0
    xproject = xsession.projects[project]

    
    dbsnapshot = pd.DataFrame(columns=["MRN"])
    dbsnapshot = pd.DataFrame(columns=["subjects","seriesName","uri","date-series","date-session","AccessionNumber"])
    for xsubject in xproject.subjects.values() :
        xmrn = xsubject.label
        xnat_subject = xproject.subjects[xmrn]
        print(xnat_subject)
        for xsession in xnat_subject.experiments.values() :            
            ses_date = xsession.date
            for xscan in xsession.scans.values() :
                #print(xscan)
                try :
                    AccessionNumber = xscan.dicom_dump(fields = "AccessionNumber")[0]["value"]
                    PID = xscan.dicom_dump(fields = "PatientID")[0]["value"]
                    PID = remove_non_numbers(PID)
                except :
                    AccessionNumber = 99
                try :
                    SeriesName = xscan.dicom_dump(fields = "SeriesDescription")[0]["value"]
                    print(SeriesName)
                except :
                    SeriesName = ""
                dbsnapshot.loc[len(dbsnapshot.index)] = ["{}".format(PID), SeriesName, xscan.uri, xscan.start_date, ses_date, AccessionNumber]
        # count = count + 1
        # if count == 5:
        #      break
    return dbsnapshot

def checkdatabasesubject(xsession, project, sdanid, xmrn) :
    #count = 0
    xproject = xsession.projects[project]
    xnat_subject = xproject.subjects[xmrn]    
    #dbsnapshot = pd.DataFrame(columns=["MRN"])
    dbsnapshot = pd.DataFrame(columns=["subjects","seriesName","uri","date-series","date-session","AccessionNumber"])
    #for xsubject in xproject.subjects.values() :
    #    xmrn = xsubject.label
    #print(xnat_subject)
    print(f"checking for {sdanid} in {project}")
    for xsession in xnat_subject.experiments.values() :            
        ses_date = xsession.date
        for xscan in xsession.scans.values() :
            #print(xscan)
            try :
                AccessionNumber = xscan.dicom_dump(fields = "AccessionNumber")[0]["value"]
                #PID = xscan.dicom_dump(fields = "PatientID")[0]["value"]
            except :
                AccessionNumber = 99
            
            SeriesName = xscan.dicom_dump(fields = "SeriesDescription")[0]["value"]
            print(SeriesName)
                         
            #dbsnapshot.loc[len(dbsnapshot.index)] = ["{}".format(PID), SeriesName, xscan.uri, xscan.start_date, ses_date, AccessionNumber]
    
            dbsnapshot.loc[len(dbsnapshot.index)] = ["{}".format(sdanid), SeriesName, xscan.uri, xscan.start_date, ses_date, AccessionNumber]
            PatientName = xscan.dicom_dump(fields = "PatientName")[0]["value"]
            PatientName = simplifystring(PatientName)
            PatientName = PatientName.split('-')
            #print(PatientName)
            # count = count + 1
        # if count == 5:
        #      break
    return dbsnapshot, PatientName

def dbreader (sdanid):
    if sdanid == 0:
        variable = subprocess.run(["dbsearch \"\""], shell = True, capture_output=True, universal_newlines = True)
        listsid = str(variable.stdout)
        listsid = listsid.replace("  ","")
        listsid = listsid.split("\n")
        listsid = listsid
        listsid = listsid[1:]
        templist = pd.DataFrame(listsid[1:])
        templist.loc[:,0] = templist.loc[:,0].str.strip()
        dbsearched = pd.DataFrame(templist.loc[:,0].str.split(" ").tolist())
        
    else :
        variable = subprocess.run(["dbsearch {}".format(sdanid)], shell = True, capture_output=True, universal_newlines = True)
        listsid = str(variable.stdout)
        listsid = listsid.replace("  ","")
        listsid = listsid.split("\n")
        listsid = listsid
        listsid = listsid[1:]
        templist = pd.DataFrame(listsid[1:])
        templist.loc[:,0] = templist.loc[:,0].str.strip()
        dbsearched = pd.DataFrame(templist.loc[:,0].str.split(" ").tolist())

    return dbsearched 

def anonymize(path_dcm, downloaddir, sdanid) :
    print('working on dicoms. Please do not interrupt.')
    paths = []
    print(os.path.join(path_dcm,"sub-{}".format(sdanid)))
    
    for root, dirs, files in os.walk(os.path.join(path_dcm,"sub-{}".format(sdanid))):
       #print(dirs)
       count = 1
       if "physio" in root :
           for f in files :
               #print(os.path.join(root,f))
               os.makedirs(os.path.join(root.replace(path_dcm,downloaddir)),exist_ok=True)
               #print(os.path.join(root.replace(path_dcm,downloaddir),f))
               copy2(os.path.join(root,f),
                     os.path.join(root.replace(path_dcm,downloaddir),f))
       if len(files) > 0:
           files = natsorted(files)
       for file in files:
          print(file)
          if file.endswith("dcm"):
             paths.append(os.path.join(root, file))
             
             #print("working on file: {}".format(file))
             ds = pydicom.filereader.dcmread(os.path.join(root, file))
             #print(ds.PatientName)
             ds.PatientName = "sub-{}".format(sdanid)
             ds.PatientID = "sub-{}".format(sdanid)
             ds.PatientBirthDate = ""
             #SN = ds.SeriesName 
             #print(ds.PatientName)
             os.makedirs(root.replace(path_dcm,downloaddir), exist_ok=True)

             ds.save_as(os.path.join(root.replace(path_dcm,downloaddir),"sub-{}_{}_{}_rec-anonymized.dcm".format(sdanid,os.path.basename(root),count)))
             #print("{} to {}".format(os.path.join(root, file),os.path.join(root.replace(path_dcm,downloaddir),"sub-{}_{}_{}_rec-anonymized.dcm".format(sdanid,os.path.basename(root),count))))
             count = count+1
             #subprocess.run(["rm {}".format(os.path.join(root, file))], shell=True)

def dcmnii(cmd):
    subprocess.run(cmd)
       
def convert2nii(path_dcm, downloaddir, sdanid,nworkers=2) :
    cmds = []
    #print(os.path.join(path_dcm,"sub-{}".format(sdanid)))
    for root, dirs, files in os.walk(os.path.join(path_dcm,"sub-{}".format(sdanid))):
        if not dirs:
            print(root, "converting")
            if os.path.isdir(root.replace(path_dcm,downloaddir)):
                continue
            os.makedirs(root.replace(path_dcm,downloaddir))
            
            dcmprocess = ["dcm2niix","-f",f"sub-{sdanid}_%f","-z","y","-o",f"{root.replace(path_dcm,downloaddir)}",f"{root}"]
            cmds.append(dcmprocess)
            # for proc in dcmprocess:
            #     	proc.wait()
        if "physio" in root :
            for f in files :
                print(os.path.join(root,f))
                copy2(os.path.join(root,f), root.replace(path_dcm,downloaddir))
    #print(cmds)
    with multiprocessing.Pool(processes=nworkers) as pool:
        pool.map(dcmnii, cmds)

def convert2nii2(path_dcm, downloaddir, sdanid) :
    #cmds = []
    #print(os.path.join(path_dcm,"sub-{}".format(sdanid)))
    for root, dirs, files in os.walk(os.path.join(path_dcm,"sub-{}".format(sdanid))):
        if not dirs:
            print(root, "converting")
            if os.path.isdir(root.replace(path_dcm,downloaddir)):
                continue
            os.makedirs(root.replace(path_dcm,downloaddir),exist_ok=True)
            
            dcmprocess = ["dcm2niix","-f",f"sub-{sdanid}_%f","-z","y","-o",f"{root.replace(path_dcm,downloaddir)}",f"{root}"]
            #cmds.append(dcmprocess)
            subprocess.run(dcmprocess)
            # for proc in dcmprocess:
            #     	proc.wait()
        if "physio" in root :
            for f in files :
                print(os.path.join(root,f))
                copy2(os.path.join(root,f), root.replace(path_dcm,downloaddir))
    #print(cmds)
   


def download_dcm_noid(xsession, project, date, seriesName, downloaddir, unzip, physio) :
    #print(xmrn)
    xproject = xsession.projects[project]
    exclude_strings = ["REQUISITION", "SCREEN-SAVE","SAVE",".txt","README"]
    #the builtin filter doesn't work. Will transform into pandas and use that to find.

    listsdanid = []
    projectdf = pd.DataFrame(xproject.experiments.tabulate())
    projectdf["date"] = pd.to_datetime(projectdf["date"])
    projectdf["date"] = projectdf["date"].dt.strftime("%m-%d-%Y")
    for d in date :
        scanses = projectdf.loc[ projectdf["date"] == d ]
        for sesref in scanses["ID"] :
        #ses_date = xsession.date
            xsession = xproject.experiments[sesref]
            ses_date = xsession.date
            MRN = str(xsession.dcm_patient_id)
            # get the sdan id
            MRN = '-'.join(MRN[i:i + 2] for i in range(0, len(MRN), 2))
            
            #variable = subprocess.run(["dbsearch \"\" | grep {}".format(MRN)], shell = True, capture_output=True, universal_newlines = True)
            #listsid = str(variable.stdout)
            #listsid = listsid.replace("  ","")
            #listsid = listsid.split("\n")
            #listsid = listsid[0:]
            #templist = pd.DataFrame(listsid[0:])
            #templist.loc[:,0] = templist.loc[:,0].str.strip()
            #dbsearched = pd.DataFrame(templist.loc[:,0].str.split(" ").tolist())
            dbsearched = checkrobin2(MRN)
            sdanid = dbsearched.loc[0,"sdan_id"]
            listsdanid = listsdanid + [sdanid]
            #print (listsdanid)
            #print(xsession.date)
            for xscan in xsession.scans.values() :
                  xsname =  xscan.series_description
                  print(xsname)
                  if any(series in xsname for series in seriesName): 
                        xsname = simplifystring(xsname)
                        xsnumber = xscan.id
                        print(xsnumber)
                        if any(series in xsname for series in exclude_strings):
                            continue
                            #xsname = simplifystring(xsname)
                        os.makedirs(os.path.join(downloaddir,"sub-{}".format(sdanid),ses_date.strftime("%m-%d-%Y")),  exist_ok = True) 
                        downloadpath = os.path.join(downloaddir,"sub-{}".format(sdanid),ses_date.strftime("%m-%d-%Y"),
                                                    "{}_{}.tgz".format(xsname,xsnumber))
                        print(downloadpath)
                        xscan.download(downloadpath)
                        if unzip : 
                            extractpath = os.path.join(downloaddir,"sub-{}".format(sdanid),ses_date.strftime("%m-%d-%Y"),
                                                        "{}_{}".format(xsname,xsnumber))
                            #os.system("unzip -j {} -d {}".format(downloadpath,extractpath))
                            subprocess.run(["unzip -j {} -d {}".format(downloadpath,extractpath)], shell=True, stdout=subprocess.DEVNULL)
                            subprocess.run(["sortme {}".format(extractpath)], shell=True)
                        if physio :
                           downloadpath = os.path.join(downloaddir,"sub-{}".format(sdanid),ses_date.strftime("%m-%d-%Y"))
                           downloadphysio(xsession,downloadpath) 
    return listsdanid

def download_dcmname(xsession, project, FirstName, LastName, sdanid, date, seriesName, downloaddir, unzip) :
    print('Matching XNAT entry by name')
    print('please check data carefully')
    xproject = xsession.projects[project]
    for i in xproject.subjects.values() :
        #print(i.label)
        NAMES = [FirstName.upper() , LastName.upper()]
        #if (FirstName.upper() and LastName.upper()) in i.label.upper() : This would match either first or lastname.
        if all(n in i.label.upper() for n in NAMES) :
            print('found subject')
            xnat_subject = i
            #print(i.label)
            #print(FirstName.upper())
            #print(LastName.upper())
            for xsession in xnat_subject.experiments.values() :
                ses_date = xsession.date
                #print(ses_date)
                if date in ses_date.strftime("%m-%d-%Y") : # if date undefined - check all sessions          
                    for xscan in xsession.scans.values() :
                      xsname =  xscan.series_description
                      if any(series in xsname for series in seriesName):
                          xsname = simplifystring(xsname)
                          xsnumber = xscan.id
                          if xsname not in ["REQUISITION", "SCREEN-SAVE"] :
                              #xsname = simplifystring(xsname)
                              os.makedirs(os.path.join(downloaddir,"sub-{}".format(sdanid),ses_date.strftime("%m-%d-%Y")),  exist_ok = True) 
                              downloadpath = os.path.join(downloaddir,"sub-{}".format(sdanid),ses_date.strftime("%m-%d-%Y"),
                                                          "{}_{}.tgz".format(xsname,xsnumber))
                              xscan.download(downloadpath)
                              if unzip : 
                                  extractpath = os.path.join(downloaddir,"sub-{}".format(sdanid),ses_date.strftime("%m-%d-%Y"),
                                                              "{}_{}".format(xsname,xsnumber))
                                  #os.system("unzip -j {} -d {}".format(downloadpath,extractpath))
                                  subprocess.run(["unzip -j {} -d {}".format(downloadpath,extractpath)], shell=True, stdout=subprocess.DEVNULL )
                                  subprocess.run(["sortme {}".format(extractpath)], shell=True)

def makebids(downloaddir,tempdir,renumber):
    print('starting BIDS')
    target = os.path.split(downloaddir)[0]
    if renumber :
        subprocess.run(["xnat2bids -n {} -b {}/BIDS -r".format(downloaddir,tempdir)],shell = True)
    else :
        subprocess.run(["xnat2bids -n {} -b {}/BIDS ".format(downloaddir,tempdir)],shell = True)
    subprocess.run(["rsync -av {}/BIDS {} -r".format(tempdir,target)],shell = True)

def contains_number(string):
    mrn_true = all(char.isdigit() for char in string.replace("-",""))
    return mrn_true

def move_to_dest(tempdir,finaldir):
    subprocess.run(["mv {}/* {}".format(tempdir,finaldir)], shell=True, stdout=subprocess.DEVNULL)
    
def simplifystring(S):  
    special_chars = [" ", "-", ".", "+", "(", ")", "/",":","!","#",
                     "$","%","^","&","*","'","`"]
    for c in special_chars:
        S = S.replace(c, "-")
    while '--' in S:
        S = S.replace("--","-")
    S = S.upper()
    return S

def remove_non_numbers(string):
    return re.sub('[^0-9]+', '', string)

# def processString(txt):
#   specialChars = "!#$%^&*()" 
#   for specialChar in specialChars:
#     txt = txt.replace(specialChar, '')
#   return txt