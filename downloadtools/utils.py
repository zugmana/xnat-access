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
#import xnat
def download_dcm(xsession, project, xmrn, sdanid, date, seriesName, downloaddir, unzip) :
    #print(xmrn)
    xproject = xsession.projects[project]
    xnat_subject = xproject.subjects[xmrn]
    for xsession in xnat_subject.experiments.values() :
        ses_date = xsession.date
        if date in ses_date.strftime("%m-%d-%Y") : # if date undefined - check all sessions          
          for xscan in xsession.scans.values() :
              xsname =  xscan.series_description
              if seriesName in xsname : 
                  xsname = processString(xsname)
                  xsnumber = xscan.id
                  if xsname not in ["Requisition"] :
                      os.makedirs(os.path.join(downloaddir,"sub-s{}".format(sdanid),ses_date.strftime("%m-%d-%Y")),  exist_ok = True) 
                      downloadpath = os.path.join(downloaddir,"sub-s{}".format(sdanid),ses_date.strftime("%m-%d-%Y"),
                                                  "{}_{}.tgz".format(xsname.replace(" ","_"),xsnumber))
                      xscan.download(downloadpath)
                      if unzip : 
                          extractpath = os.path.join(downloaddir,"sub-s{}".format(sdanid),ses_date.strftime("%m-%d-%Y"),
                                                      "{}_{}".format(xsname.replace(" ","_"),xsnumber))
                          #os.system("unzip -j {} -d {}".format(downloadpath,extractpath))
                          subprocess.run(["unzip -j {} -d {}".format(downloadpath,extractpath)], shell=True)
                          subprocess.run(["rm {}".format(downloadpath)], shell=True)

def checkdatabase(xsession, project) :
    count = 0
    xproject = xsession.projects[project]

    
    dbsnapshot = pd.DataFrame(columns=["MRN"])
    dbsnapshot = pd.DataFrame(columns=["subjects","seriesName","uri","date-series","date-session","AccessionNumber"])
    for xsubject in xproject.subjects.values() :
        xmrn = xsubject.label
        xnat_subject = xproject.subjects[xmrn]
        for xsession in xnat_subject.experiments.values() :
            #print(xsession)
            ses_date = xsession.date
            for xscan in xsession.scans.values() :
                print(xscan)
                AccessionNumber = xscan.dicom_dump(fields = "AccessionNumber")[0]["value"]
                dbsnapshot.loc[len(dbsnapshot.index)] = [xmrn, xscan.series_description, xscan.uri, xscan.start_date, ses_date, AccessionNumber]
        count = count + 1
        if count == 2:
            break
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

def anonymize(path_dcm, sdanid) :
    print('working on dicoms.')
    paths = []
    for root, dirs, files in os.walk(path_dcm):
       #print(dirs)
       for file in files:
          if file.endswith("dcm"):
             paths.append(os.path.join(root, file))
             print("working on file: {}".format(file))
             ds = pydicom.filereader.dcmread(os.path.join(root, file))
             print(ds.PatientName)
             ds.PatientName = sdanid
             print(ds.PatientName)
             ds.save_as(os.path.join(root, file))
             
       
def convert2nii(path_to_subj) :
    for root, dirs, files in os.walk(path_to_subj):
        if not dirs:
            print(root, "converting")        
            subprocess.run(["dcm2niix -f '%f' -z y -o {} {} ".format(root,root)], shell = True)
    
    for root, dirs, files in os.walk(path_to_subj):
       #print(dirs)
       for file in files:
          if file.endswith("dcm"):
             #paths.append(os.path.join(root, file))
             #print("working on file: {}".format(file))
             subprocess.run(["rm {} ".format(os.path.join(root, file))], shell = True)    
        

def processString(txt):
  specialChars = "!#$%^&*()" 
  for specialChar in specialChars:
    txt = txt.replace(specialChar, '')
  return txt