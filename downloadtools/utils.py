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
              if any(series in xsname for series in seriesName): 
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
    #count = 0
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
        #count = count + 1
        #if count == 2:
        #    break
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
    print('working on dicoms. Please do not interrupt.')
    paths = []
    #print(os.path.join(path_dcm,"sub-s{}".format(sdanid)))
    for root, dirs, files in os.walk(os.path.join(path_dcm,"sub-s{}".format(sdanid))):
       #print(dirs)
       count = 0
       for file in files:
          #print(file)
          if file.endswith("dcm"):
             paths.append(os.path.join(root, file))
             print("working on file: {}".format(file))
             ds = pydicom.filereader.dcmread(os.path.join(root, file))
             #print(ds.PatientName)
             ds.PatientName = "sub-s{}".format(sdanid)
             ds.PatientID = "sub-s{}".format(sdanid)
             ds.PatientBirthDate = ""
             #SN = ds.SeriesName 
             #print(ds.PatientName)
             ds.save_as(os.path.join(root,"{}_{}.dcm".format(os.path.basename(root),count)))
             count = count+1
             
       
def convert2nii(path_dcm, sdanid) :
    for root, dirs, files in os.walk(os.path.join(path_dcm,"sub-s{}".format(sdanid))):
        if not dirs:
            print(root, "converting")        
            subprocess.run(["dcm2niix -f '%f' -z y -o {} {} ".format(root,root)], shell = True)
    
    for root, dirs, files in os.walk(os.path.join(path_dcm,"sub-s{}".format(sdanid))):
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

def download_dcm_noid(xsession, project, date, seriesName, downloaddir, unzip) :
    #print(xmrn)
    xproject = xsession.projects[project]
    #xnat_subject = xproject.subjects[xmrn]
    #the builtin filter doesn't work. Will transform into pandas and use that to find.
    #date = ["2019-03-02", "2018-03-02"]
    listsdanid = []
    projectdf = pd.DataFrame(xproject.experiments.tabulate())
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
                        if xsname not in ["Requisition"] :
                            os.makedirs(os.path.join(downloaddir,"sub-s{}".format(sdanid),ses_date.strftime("%m-%d-%Y")),  exist_ok = True) 
                            downloadpath = os.path.join(downloaddir,"sub-s{}".format(sdanid),ses_date.strftime("%m-%d-%Y"),
                                                        "{}_{}.tgz".format(xsname.replace(" ","_"),xsnumber))
                            print(downloadpath)
                            xscan.download(downloadpath)
                            if unzip : 
                                extractpath = os.path.join(downloaddir,"sub-s{}".format(sdanid),ses_date.strftime("%m-%d-%Y"),
                                                            "{}_{}".format(xsname.replace(" ","_"),xsnumber))
                                #os.system("unzip -j {} -d {}".format(downloadpath,extractpath))
                                subprocess.run(["unzip -j {} -d {}".format(downloadpath,extractpath)], shell=True)
                                subprocess.run(["rm {}".format(downloadpath)], shell=True)
    return listsdanid

def download_dcmname(xsession, project, FirstName, LastName, sdanid, date, seriesName, downloaddir, unzip) :
    print('MRN not found attempting to match by name')
    print('please check data carefully')
    xproject = xsession.projects[project]
    for i in xproject.subjects.values() :
        #print(i.label)
        if FirstName.upper() and LastName.upper() in i.label.upper() :
            print('found subject')
            xnat_subject = i
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
                              os.makedirs(os.path.join(downloaddir,"sub-s{}".format(sdanid),ses_date.strftime("%m-%d-%Y")),  exist_ok = True) 
                              downloadpath = os.path.join(downloaddir,"sub-s{}".format(sdanid),ses_date.strftime("%m-%d-%Y"),
                                                          "{}_{}.tgz".format(xsname.replace(" ","_"),xsnumber))
                              xscan.download(downloadpath)
                              if unzip : 
                                  extractpath = os.path.join(downloaddir,"sub-s{}".format(sdanid),ses_date.strftime("%m-%d-%Y"),
                                                              "{}_{}".format(xsname.replace(" ","_"),xsnumber))
                                  #os.system("unzip -j {} -d {}".format(downloadpath,extractpath))
                                  subprocess.run(["unzip -j {} -d {}".format(downloadpath,extractpath)], shell=True, stdout=subprocess.DEVNULL )
                                  subprocess.run(["rm {}".format(downloadpath)], shell=True)
#check if mrn is actually a name
def contains_number(string):
    mrn_true = all(char.isdigit() for char in string.replace("-",""))
    return mrn_true