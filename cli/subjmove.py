#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 15 09:40:58 2024

@author: zugmana2
"""

import requests
#import pandas as pd
#import json
import sys
#import os
#from downloadtools.utils import simplifystring
from downloadtools.restutilities import listsession
import warnings
import getpass

def subjectmove (subjectid,sourceproject,destproject,changeprimary=True,label=None):
    #primary
    query = f"/data/projects/{sourceproject}/subjects/{subjectid}/projects/{destproject}"
    if changeprimary:
        query = query + "?primary=true"
    if label:
        if query.endswith("true"):
            query = query + f"&label={label}"
        else :
            query = query + f"?label={label}"
    return query
    #PUT - /data/projects/{original-project-id}/subjects/{subject-id | subject-label}/projects/{shared-project-id}
def experimentmove (subjectid, experiment_id,sourceproject,destproject,changeprimary=True,label=None):
    #/data/projects/{original-project-id}/subjects/{subject-id | subject-label}/experiments/{experiment-id | experiment-label}/projects/{shared-project-id}
    query = f"/data/projects/{sourceproject}/subjects/{subjectid}/experiments/{experiment_id}/projects/{destproject}"
    if changeprimary:
        query = query + "?primary=true"
    if label:
        if query.endswith("true"):
            query = query + f"&label={label}"
        else :
            query = query + f"?label={label}"
    return query


user = getpass.getuser()
print ("current user is {}".format(user))
password = getpass.getpass(prompt="Please enter Password : ")

xnaturl = "https://fmrif-xnat.nimh.nih.gov"
project = "01-M-0192"
destproject = "Testtreansfer"
with requests.sessions.Session() as connect:
    
    connect.auth = (user, password)
    connect.xnaturl = xnaturl
    connect.project = project
    connect.base_url = f'{xnaturl}/data/projects/{project}'
    # test connection
    response = connect.get(connect.base_url)
    if not response.ok:
        warnings.warn("You can't access xnat project {project} with the credenctials provided.")
        sys.exit("Ending program")
    allsessions = listsession(connect)
    #match by whatever
    #lets say study instance uid
    instances = []
    scans = allsessions[allsessions['xnat:mrsessiondata/uid'].str.contains('|'.join(instances))]
    # Get subject IDs
    xnatID = scans["xnat:mrsessiondata/subject_id"].unique()
    xnatID = xnatID[0] # in this case I know it's only one. Could loop through a list.
    query = subjectmove(xnatID,project,destproject,changeprimary=True,label=None)
    r = connect.put(f"{xnaturl}{query}")
    if r.status_code == 200:
        print("worked")
    else :
        print("fail")
    # below is not necessary unless the subject is already shared
    expID = scans["xnat:mrsessiondata/id"].to_list()
    for exp in expID:
        queryexp = experimentmove(xnatID,exp,project,destproject,changeprimary=True,label=None)
        r = connect.put(f"{xnaturl}{query}")
        if r.status_code == 200:
            print("worked")
        else :
            print("fail")