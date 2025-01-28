#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 12 14:13:55 2024

@author: zugmana2
"""
import requests
import pandas as pd
import json
import sys
import os
from downloadtools.utils import simplifystring
from downloadtools.pgsqlutils import get_home_directory
import warnings
#xnataddress = "https://fmrif-xnat.nimh.nih.gov"
#cookie = s.get("https://fmrif-xnat.nimh.nih.gov")

def listsubjects(session):
    #list all subjects
    query = f'{session.base_url}/subjects/'
    result = session.get(query)
    data = pd.DataFrame.from_dict(result.json()["ResultSet"]["Result"])
    return data

def listsession(session,xnatid = None ,date = None):
    columns = ''.join(('xnat:subjectData/label,',
                       #'xnat:subjectData/ID,',
                       'xnat:mrSessionData/label,',
                       'xnat:mrSessionData/ID,',
                       'xnat:mrSessionData/date,',
                       'xnat:mrSessionData/project,',
                       'URI,',
                       'xnat:mrSessionData/UID'))
    if xnatid == None:
        query = f'{session.base_url}/experiments/?format=json&columns={columns}'
    else:
        query = f'{session.base_url}/subjects/{xnatid}/experiments/?format=json&columns={columns}'
    
    if date == "":
        date = None
    if date:
        query = f'{query}&date={date}'
    print(query)
    result = basequery(session,query)
    data = pd.DataFrame.from_dict(result.json()["ResultSet"]["Result"])
    return data

def getxnatidfrommrn(MRN,data):
    # This is just if you got the full list first
    if type(MRN) != str:
        MRN = str(MRN)
        
    ID = data.loc[data["label"] == MRN,"ID"].values[0]
    return ID

def listscans(session,sessiontable,get_header=False):
    data = pd.DataFrame()
    datahdr = pd.DataFrame()
    for i,j in sessiontable.iterrows():
        sesID = j['xnat:mrsessiondata/id']
        xnatid = j['xnat:mrsessiondata/subject_id']
        # columnsstring = ''.join(('xnat:mrSessionData/label,',
        #                          'xnat:mrSessionData/ID,',
        #                          'xnat:mrSessionData/date,',
        #                          'xnat:mrScanData'))
        # columns = ''.join(('xnat:mrScanData'))
        query = f'{session.base_url}/subjects/{xnatid}/experiments/{sesID}/scans?format=json'
        print(query)
        result = session.get(query)
        datases = pd.DataFrame.from_dict(result.json()["ResultSet"]["Result"])
        datases["UID"] = ""
        for ii,jj in datases.loc[datases["xsiType"] == "xnat:mrScanData"].iterrows():
            columns = ''.join(('xnat:mrSessionData/label,',
                                      'xnat:mrSessionData/ID,',
                                      'xnat:mrSessionData/date,',
                                      'xnat:mrScanData,',
                                      'UID,',
                                      'series_description,',
                                      'type,',
                                      'xnat:imageScanData/quality,',
                                      'scanner'))
            query = f'{session.base_url}/subjects/{xnatid}/experiments/{sesID}/scans/{jj["ID"]}?format=json&columns={columns}'
            print(query)
            result = session.get(query)
            datascan = pd.DataFrame.from_dict(result.json()["items"][0]['data_fields'], orient='index').T
            #print(datascan)
            if "UID" in datascan.columns:    
                datases.loc[ii,"UID"] = datascan.loc[0,"UID"]
            else:
                datases.loc[ii,"UID"] = "NaN"
        datases["sesID"] = sesID
        datases["subject_label"] = j["subject_label"]
        datases["xnatID"] = j['xnat:mrsessiondata/subject_id']
        datases["project"] = j['xnat:mrsessiondata/project']
        datases["date"] = j['xnat:mrsessiondata/date']
        datases["session_UID"] =j['xnat:mrsessiondata/uid']
            
        #datases.set_index(['sesID'], inplace=True)
        if get_header:
            #datahdr[sesID] = {}
             
            for ii,jj in datases.loc[datases["xsiType"] == "xnat:mrScanData"].iterrows():
                #print(jj["xsiType"])
                seriesnumber = jj["ID"]
                #datahdr[sesID][seriesnumber] = {}
                #print(jj)
                #print(seriesnumber)
                hdr = getdicomdump(session,xnatid,sesID,seriesnumber,fields=["PatientID", "PatientName","AcquisitionDate"])
                #print(hdr)
                #print(seriesnumber)
                #result3= getdicomdum2(session,xnatid,sesID,seriesnumber)
                #print(hdr)
                hdr["seriesnumber"] = seriesnumber
                hdr["URI"] = jj["URI"]
                hdr["sesID"] = jj["sesID"]
                hdr["mrn"] = jj["subject_label"]
                hdr["series_description"] = jj["series_description"]
                hdr["xnatID"] = jj['xnatID']
                hdr["project"] = jj['project']
                datahdr = pd.concat([datahdr.reset_index(drop=True), hdr.reset_index(drop=True)])
                
        data = pd.concat([data,datases])
    return data,datahdr
# def listscansdate(session,sessiontable,xnatid,date):
#     data = pd.DataFrame()
#     datahdr = {}
#     for i,j in sessiontable.iterrows():
#         sesID = j["ID"]
#         query = f'{session.base_url}/subjects/{xnatid}/experiments/{sesID}/'
#         result = session.get(query)
# def getdicomdum2(session,xnatid,sesID,seriesnumber):
#     query = f'{session.xnaturl}/data/services/dicomdump?src=/archive/projects/{session.project}/subjects/{xnatid}/experiments/{sesID}/scans/{seriesnumber}/xnat:mrScanData'
#     print(query)
#     result = session.get(query)

def getdicomdump(session,xnatid,sesID,seriesnumber,fields=None):
    
    query = f'{session.xnaturl}/data/services/dicomdump?src=/archive/projects/{session.project}/subjects/{xnatid}/experiments/{sesID}/scans/{seriesnumber}'
    if fields:
        for field in fields:
            query += f"&field={field}"
    #print(query)
    result = session.get(query)
    #datahdr = pd.DataFrame.from_dict(result.json()["ResultSet"]["Result"])
    b = result.json()["ResultSet"]["Result"]
    name = [r["value"] for r in b ]
    desc = [r["desc"] for r in b ]
    df = pd.DataFrame([name], columns=desc)
    df = df.drop(columns=[col for col in df.columns if col == '?'])
    return df

def getsuplist(session,sessiontable):
    #/data/projects/{project-id}/files
    #/data/projects/TEST/subjects/1/experiments/MR1/scans/1/files
    data = pd.DataFrame()
    for i,j in sessiontable.iterrows():
        sesID = j['xnat:mrsessiondata/id']
        xnatid = j['xnat:mrsessiondata/subject_id']
        query = f'{session.xnaturl}/data/projects/{session.project}/subjects/{xnatid}/experiments/{sesID}/files'
    
        result = session.get(query)
        datases = pd.DataFrame.from_dict(result.json()["ResultSet"]["Result"])
        datases["sesID"] = sesID
        datases["subject_label"] = j["subject_label"]
        datases["xnatID"] = j['xnat:mrsessiondata/subject_id']
        datases["project"] = j['xnat:mrsessiondata/project']
        datases["date"] = j['xnat:mrsessiondata/date']
        data = pd.concat([data,datases])
        
    #datahdr = datahdr.T
    #datahdr.columns = datahdr.iloc[1]
    #datahdr = datahdr.drop(datahdr.index[1])
    return data

def getdicomlist(session,xnatid,sesID,seriesnumber):
    #/data/projects/{project-id}/files
    #/data/projects/TEST/subjects/1/experiments/MR1/scans/1/files
    query = f'{session.xnaturl}/data/projects/{session.project}/subjects/{xnatid}/experiments/{sesID}/scans/{seriesnumber}/resources/DICOMfiles'
    print(query)
    result = session.get(query)
    datahdr = pd.DataFrame.from_dict(result.json()["ResultSet"]["Result"])
    
    #datahdr = datahdr.T
    #datahdr.columns = datahdr.iloc[1]
    #datahdr = datahdr.drop(datahdr.index[1])
    return result,datahdr
def getphysioforsesseions(session,sessiontable):
    for i,j in sessiontable.iterrows():
        sesID = j['xnat:mrsessiondata/id']
        xnatid = j['xnat:mrsessiondata/subject_id']
        result,datahdr = getphysiolist(session,xnatid,sesID)
        #print(datahdr)
        return result,datahdr
def getphysiolist(session,xnatid,sesID):
    query = f'{session.xnaturl}/data/projects/{session.project}/subjects/{xnatid}/experiments/{sesID}/resources/supplementary/files?format=json'
    #print(query)
    result = session.get(query)
    try :
        datahdr = pd.DataFrame.from_dict(result.json()["ResultSet"]["Result"])
    except :
        print(f"{query} returned invalid value")
        datahdr = []
    return result,datahdr
def tupletodownload(baseurl,fileuri,destination,cookies):
    #print(a)
    #baseurl,fileuri,destination,cookies = a
    print(f"downloadfile({baseurl},{fileuri},{destination},cookies,zipped=True)\n")

def downloadfile(baseurl,fileuri,destination,cookies,zipped=True):
    query = f'{baseurl}{fileuri}'
    
    if zipped:
        query = f'{query}?format=zip'
    print(query)
    path = os.path.dirname(destination)
    os.makedirs(path,exist_ok=True)
    #print(query)
    #print("starting download. Will take a while")
    cookies = requests.utils.cookiejar_from_dict(cookies)
    session = requests.session()
    session.cookies.update(cookies)
    r = session.get(query, stream=True)
    chunk_size = 128
    with open(destination, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)

def find_dict_with_field(list_of_dicts):
    for dictionary in list_of_dicts:
        if 'field' in dictionary and dictionary['field'] == 'scans/scan':
            return dictionary
    return None
def queryxnatID(MRN,session):
    query = f'{session.base_url}/subjects/?label={MRN}&format=json'
    result = basequery(session,query)
    data = pd.DataFrame.from_dict(result.json()["ResultSet"]["Result"])
    if len(data) < 1:
        print(f'{MRN} not found in project: {session.project}')
        data = None
    return data
def basequery(session,querytext):
    result = session.get(querytext)
    if result.ok:
        return result
    else:
        print("there was an error connecting. please check credentials and if you have access to project")
        sys.exit()
        return

def namecheck(robinresult,xnatname):
    PatientName = xnatname.split('-')
    FirstName = simplifystring(robinresult.loc[0,"first_name"])
    FirstName = FirstName.split('-')
    LastName = simplifystring(robinresult.loc[0,"last_name"])
    LastName = LastName.split('-')
    NamesRobin = FirstName + LastName
    mismatched_names = [name for name in NamesRobin if name not in PatientName]

    # Check if there are any mismatches
    if mismatched_names:
     
     for name in mismatched_names:
         print(name)
         
         warnings.warn(f"Warning: The following names from list ROBIN are not in the DICOM PatientName field: {name}")
    mismatched_names = [name for name in PatientName if name not in NamesRobin]

    # Check if there are any mismatches
    if mismatched_names:
    
     for name in mismatched_names:
         print(name)
         warnings.warn(f"Warning: The following names from list DICOM PatientName are not in the ROBIN field: {name}")
def read_config_connect():
    home_dir = get_home_directory()
    config_file = os.path.join(home_dir, '.config.xnat')

    if os.path.exists(config_file):
        with open(config_file, 'r') as f:
            config = json.load(f)
    else:
        config = {}
        config['login_method'] = input("Enter value for login_method: ")
        with open(config_file, 'w') as f:
            json.dump(config, f)

    return config