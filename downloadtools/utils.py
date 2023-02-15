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
#import xnat
def downloadphysio(xobject,downloadpath):
    #splitpath = os.path.split(downloadpath)
    print(xobject)
    #file = splitpath[1].replace("tgz","_physio.tsv")
    physiopath = os.path.join(downloadpath,"physio")
    os.makedirs(physiopath, exist_ok = True)
    for r in xobject.resources.values() :
        for file in r.files.values():
            if "ECG" in file.data["Name"] :
                print("found physio file {}".format(file.data["Name"]))
                file.download(os.path.join(physiopath,file.data["Name"]))
            if "Resp" in file.data["Name"] :
                print("found physio file {}".format(file.data["Name"]))
                file.download(os.path.join(physiopath,file.data["Name"]))
            else :
                print("Description file {}".format(file.data["Name"]))
                file.download(os.path.join(physiopath,file.data["Name"]))


def download_dcm(xsession, project, xmrn, sdanid, date, seriesName, downloaddir, unzip) :
    #print(xmrn)
    xproject = xsession.projects[project]
    xnat_subject = xproject.subjects[xmrn]
    count = 0
    for xsession in xnat_subject.experiments.values() :
        ses_date = xsession.date
        #print(ses_date)
        if date in ses_date.strftime("%m-%d-%Y") : # if date undefined - check all sessions          
          for xscan in xsession.scans.values() :
              xsname =  xscan.series_description
              if any(series in xsname for series in seriesName): 
                  xsname = processString(xsname)
                  xsnumber = xscan.id
                  if xsname not in ["Requisition", "Screen Save"] :
                      xsname = simplifystring(xsname)
                      os.makedirs(os.path.join(downloaddir,"sub-{}".format(sdanid),ses_date.strftime("%m-%d-%Y")),  exist_ok = True) 
                      downloadpath = os.path.join(downloaddir,"sub-{}".format(sdanid),ses_date.strftime("%m-%d-%Y"),
                                                  "{}_{}.tgz".format(xsname,xsnumber))
                      xscan.download(downloadpath)
                      count = count + 1
                      
                      if unzip : 
                          extractpath = os.path.join(downloaddir,"sub-{}".format(sdanid),ses_date.strftime("%m-%d-%Y"),
                                                      "{}_{}".format(xsname,xsnumber))
                          subprocess.run(["unzip -j {} -d {}".format(downloadpath,extractpath)], shell=True, stdout=subprocess.DEVNULL)
                          subprocess.run(["sortme {}".format(extractpath)], shell=True)
          if count > 0 :
              downloadpath = os.path.join(downloaddir,"sub-{}".format(sdanid),ses_date.strftime("%m-%d-%Y"))
              downloadphysio(xsession,downloadpath)
            
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
                except :
                    AccessionNumber = 99
                dbsnapshot.loc[len(dbsnapshot.index)] = ["{}".format(PID), xscan.series_description, xscan.uri, xscan.start_date, ses_date, AccessionNumber]
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
    print(xnat_subject)
    for xsession in xnat_subject.experiments.values() :            
        ses_date = xsession.date
        for xscan in xsession.scans.values() :
            #print(xscan)
            try :
                AccessionNumber = xscan.dicom_dump(fields = "AccessionNumber")[0]["value"]
                #PID = xscan.dicom_dump(fields = "PatientID")[0]["value"]
            except :
                AccessionNumber = 99
            dbsnapshot.loc[len(dbsnapshot.index)] = ["{}".format(sdanid), xscan.series_description, xscan.uri, xscan.start_date, ses_date, AccessionNumber]
            # count = count + 1
        # if count == 5:
        #      break
    return dbsnapshot

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
    #print(os.path.join(path_dcm,"sub-{}".format(sdanid)))
    for root, dirs, files in os.walk(os.path.join(path_dcm,"sub-{}".format(sdanid))):
       #print(dirs)
       count = 0
       for file in files:
          #print(file)
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
             count = count+1
             #subprocess.run(["rm {}".format(os.path.join(root, file))], shell=True)
       
def convert2nii(path_dcm, downloaddir, sdanid) :
    for root, dirs, files in os.walk(os.path.join(path_dcm,"sub-{}".format(sdanid))):
        if not dirs:
            print(root, "converting")
            os.makedirs(root.replace(path_dcm,downloaddir), exist_ok=True)
            subprocess.run(["dcm2niix -f 'sub-{}_%f' -z y -o {} {} ".format(sdanid,root.replace(path_dcm,downloaddir),root)], shell = True)
        if "physio" in root :
            for f in files :
                print(os.path.join(root,f))
                copy2(os.path.join(root,f), root.replace(path_dcm,downloaddir))

def processString(txt):
  specialChars = "!#$%^&*()" 
  for specialChar in specialChars:
    txt = txt.replace(specialChar, '')
  return txt

def download_dcm_noid(xsession, project, date, seriesName, downloaddir, unzip) :
    #print(xmrn)
    xproject = xsession.projects[project]

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
            variable = subprocess.run(["dbsearch \"\" | grep {}".format(MRN)], shell = True, capture_output=True, universal_newlines = True)
            listsid = str(variable.stdout)
            listsid = listsid.replace("  ","")
            listsid = listsid.split("\n")
            listsid = listsid[0:]
            templist = pd.DataFrame(listsid[0:])
            templist.loc[:,0] = templist.loc[:,0].str.strip()
            dbsearched = pd.DataFrame(templist.loc[:,0].str.split(" ").tolist())
            sdanid = dbsearched.loc[0,0]
            listsdanid = listsdanid + [sdanid]
            #print (listsdanid)
            #print(xsession.date)
            for xscan in xsession.scans.values() :
                  xsname =  xscan.series_description
                  print(xsname)
                  if any(series in xsname for series in seriesName): 
                        xsname = processString(xsname)
                        xsnumber = xscan.id
                        print(xsnumber)
                        if xsname not in ["Requisition", "Screen Save"] :
                            xsname = simplifystring(xsname)
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
                          xsname = processString(xsname)
                          xsnumber = xscan.id
                          if xsname not in ["Requisition", "Screen Save"] :
                              xsname = simplifystring(xsname)
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
    special_chars = [" ", "-", ".", "+", "(', ')", "/",":"]
    for c in special_chars:
        S = S.replace(c, "-")
    while '--' in S:
        S = S.replace("--","-")
    return S