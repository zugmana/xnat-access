#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct  1 11:05:55 2024

@author: zugmana2
"""

# This script will take all DICOMS on a folder, convert to nii and transform those to nii
# The series Description will be a modifier indicating the datatype.
# You can either run it from spyder or as a command.
# Make sure to install the dependencies first.

import os
import subprocess
import pandas as pd
import argparse
from pydicom import dcmread
from pathlib import Path
import sys
import pandas as pd
import json
from natsort import natsorted
from bids.layout.writing import build_path
import re
from shutil import copy2,rmtree
# 1 - convert dicom to nii
# 2 - ask user for new id. if blank use MRN
# 3 - Put into bids format.


def arguments():
        parser = argparse.ArgumentParser(description="This is a simplyfied version to be used on data that is already downloaded. Created by Andre Zugman")
        parser.add_argument('-i', '--id', nargs='+',dest="id", action='store', type=str, required=False,
                    help='id of subject to replace what is in the DICOM. If left blank will use information stored in the dicom headers', default=None)
        parser.add_argument('-o','--output', action='store', type=str, required=True,
                            help='output path.')
        parser.add_argument('-f','--folder', action='store', type=str, required=True,
                            help='output folder.')
        return parser
        
def generate_file_dataframe(root_dir,filterdcm=True): # list dicoms while skipping other stuff
    folder_paths = []
    file_paths = []
    
    # Traverse directory tree using os.walk
    for folderpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if filterdcm :
                if filename.endswith("dcm"):
                    folder_paths.append(folderpath)
                    file_paths.append(os.path.join(folderpath, filename))
            else :
                folder_paths.append(folderpath)
                file_paths.append(os.path.join(folderpath, filename))
    # Create a DataFrame
    df = pd.DataFrame({
        'folderpath': folder_paths,
        'filepath': file_paths
    })
    
    return df


def simplifystring(S):  
    special_chars = [" ", "-", ".", "+", "(", ")", "/",":","!","#",
                     "$","%","^","&","*","'","`"]
    for c in special_chars:
        S = S.replace(c, "")
    while '--' in S:
        S = S.replace("--","")
    S = S.upper()
    return S

def readjson(jsonfile): # =====================================================
    with open(jsonfile, 'r') as fp:
        try:
            J = json.load(fp)
        except json.JSONDecodeError:
            print("############################################################")
            print("{} json is invalid - please check carefully".format(jsonfile))
            print("Will skip json and associated files. Please check carefully.")
            print("############################################################")
            J = {}
    return J
def writejson(J, jsonfile): # =================================================
    with open(jsonfile, 'w') as fp:
        json.dump(J, fp, indent=2)
    return
def get_bids_path(a):
    PATTERN = ["sub-{subject}[/ses-{session}]/{datatype<anat|func|fmap>}/sub-{subject}[_ses-{session}][_task-{task}][_acq-{acquisition}][_rec-{reconstruction}][_dir-{direction}][_run-{run}][_echo-{echo}]_{suffix<bold|T1w|T2w|epi|sbref>}{extension<.tsv|.nii.gz|.json>}"]
    filesnet = build_path(a,PATTERN)
    return Path(filesnet)
def main():
    if hasattr(sys, "ps1"):
        parser = {}
        parser["id"] = None
        parser["folder"] = Path('/Path/to/Data/MRXXXXXX/')
        parser["output"] = Path('/Path/to/BIDS/')
    else :
        parser = arguments()
        parser["folder"] = Path(parser["folder"])
        parser["output"] = Path(parser["output"])
    ses = None # Edit here if you want to specify session number.
    # First do a walk to get a list of all files and paths.
    dirlist = generate_file_dataframe(parser["folder"])

    for i,ii in dirlist.groupby(by="folderpath"):
        print(i) # folder
        
        ds = dcmread(ii.iloc[0]["filepath"]) # 
        if not parser["id"]:
           parser["id"] = ds.PatientID
        SeriesDescription = simplifystring(ds.SeriesDescription)
        SeriesNumber = ds.SeriesNumber
        outfile = parser["output"] / parser["id"] / SeriesDescription
        if not outfile.exists():
            os.makedirs(outfile)
        dcmprocess = ["dcm2niix","-f",f"sub-{parser['id']}_{SeriesDescription}_{SeriesNumber}","-z","y","-o",f"{str(outfile)}",f"{i}"] # This is the command to convert. Edit here if needed.       
        subprocess.run(dcmprocess)
    # now after conversion go through output directory.
    # list files again
    outlist = generate_file_dataframe(parser["output"] / parser["id"] , filterdcm=False)
    # Get rid of bvals and bvecs. These files are not necessary
    # Work only with the jsons (because it's easier)
    outlist = outlist.loc[outlist["filepath"].str.endswith("json")]
    # Create bids names.
    # first let's find each type of file
    matchingfmaps = outlist.loc[outlist["filepath"].str.contains("MATCHING")]
    oppositefmaps = outlist.loc[outlist["filepath"].str.contains("OPPOSITE")]
    T1w = outlist.loc[(outlist["filepath"].str.contains("T1W")) & (outlist["filepath"].str.contains("ORIG"))]    # Keep only ORIG
    T2w = outlist.loc[(outlist["filepath"].str.contains("T2W")) & (outlist["filepath"].str.contains("ORIG"))]
    MEepi = outlist.loc[(outlist["filepath"].str.contains("EPI25")) &
                                (~ outlist["filepath"].str.contains("MATCHING")) &
                                (~ outlist["filepath"].str.contains("OPPOSITE"))]
    runcount = 0
    RunNumber = 0
    os.makedirs(parser["output"] / f"sub-{parser['id']}", exist_ok=True)
    for i,ii in enumerate(natsorted(MEepi["filepath"])): # this will put the files in order of acquisition, making things easier.
       
        RunNumbernew = int(ii.split("_")[-2])
        if RunNumbernew > RunNumber:
            RunNumber = RunNumbernew
            runcount = runcount + 1
        echonumber = int("".join(re.findall(r"\d+", ii.split("_")[-1])))
        a = {"subject" :parser['id'] ,
             "session" : ses,
             "run" : str(runcount),
             "echo" : str(echonumber),
             "datatype": "func",
             "suffix" : "bold",
             "extension":".nii.gz",
             "task" : "rest"}
        #print(RunNumber)
        print(ii.replace(".json",".nii.gz"))
        print("will be saved as \n")
        print(get_bids_path(a))
        newfile = get_bids_path(a)
        #MEepi.loc[MEepi["filepath"] == ii, "newname"] = newfile
        bidsdir = parser["output"] / newfile.parent
        if not bidsdir.exists():
            bidsdir.mkdir()
        copy2(ii.replace(".json",".nii.gz"),parser["output"] / newfile)
        #copy2(ii,str(parser["output"] / newfile).replace(".nii.gz", ".json")) # Instead of copying let's edit and save with B0 Field
        J = readjson(ii)
        J["B0FieldSource"] = "Field1" + (f"ses{ses}" if ses is not None else "")
        # This will always use Field 1 # Need to manually adjust if you want something different
        J["TaskName"] = "rest"
        writejson(J, str(parser["output"] / newfile).replace(".nii.gz", ".json"))
    # Now t1w
    runcount = 0
    RunNumber = 0
    os.makedirs(parser["output"] / f"sub-{parser['id']}", exist_ok=True)
    for i,ii in enumerate(natsorted(T1w["filepath"])): # this will put the files in order of acquisition, making things easier.
       
        RunNumbernew = int(ii.split("_")[-1].replace(".json",""))
        if (RunNumbernew > RunNumber) and (i > 1):
            RunNumber = RunNumbernew
            runcount = runcount + 1
        if len(T1w["filepath"]) == 1:
            runcount = None
        a = {"subject" :parser['id'] ,
             "session" : ses,
             "run" : runcount,
             "datatype": "anat",
             "suffix" : "T1w",
             "extension":".nii.gz",
             }
        #print(RunNumber)
        print(ii.replace(".json",".nii.gz"))
        print("will be saved as \n")
        print(get_bids_path(a))
        newfile = get_bids_path(a)
        #T1w.loc[T1w["filepath"] == ii, "newname"] = newfile
        bidsdir = parser["output"] / newfile.parent
        if not bidsdir.exists():
            bidsdir.mkdir()
        copy2(ii.replace(".json",".nii.gz"),parser["output"] / newfile)
        copy2(ii,str(parser["output"] / newfile).replace(".nii.gz", ".json"))
    runcount = 0
    RunNumber = 0
    for i,ii in enumerate(natsorted(T2w["filepath"])): # this will put the files in order of acquisition, making things easier.
       
        RunNumbernew = int(ii.split("_")[-1].replace(".json",""))
        if (RunNumbernew > RunNumber) and (i > 1):
            RunNumber = RunNumbernew
            runcount = runcount + 1
        if len(T2w["filepath"]) == 1:
            runcount = None
        a = {"subject" :parser['id'] ,
             "session" : ses,
             "run" : runcount,
             "datatype": "anat",
             "suffix" : "T2w",
             "extension":".nii.gz",
             }
        #print(RunNumber)
        print(ii.replace(".json",".nii.gz"))
        print("will be saved as \n")
        print(get_bids_path(a))
        newfile = get_bids_path(a)
        #T2w.loc[T2w["filepath"] == ii, "newname"] = newfile
        bidsdir = parser["output"] / newfile.parent
        if not bidsdir.exists():
            bidsdir.mkdir()
        copy2(ii.replace(".json",".nii.gz"),parser["output"] / newfile)
        copy2(ii,str(parser["output"] / newfile).replace(".nii.gz", ".json"))
    runcount = 0
    RunNumber = 0
    for i,ii in enumerate(natsorted(matchingfmaps["filepath"])): # this will put the files in order of acquisition, making things easier.
        if not ii.__contains__('e1'): # The idea of these lines is to get the first echo only. If you want something different edit this
            continue
        if not ii.__contains__('DISTORTION'): # Likewise I'm discarding the single bands for simplicity. If you want you can change this
            continue
        RunNumbernew = int(ii.split("_")[-2].replace(".json",""))
        if (RunNumbernew > RunNumber) and (i > 1):
            RunNumber = RunNumbernew
            runcount = runcount + 1
        if runcount == 0:
            runcounter = None
        else :
           runcounter = runcount
        a = {"subject" :parser['id'] ,
            "session" : ses,
            "run" : runcounter,
             "datatype": "fmap",
             "suffix" : "epi",
             "extension":".nii.gz",
             "direction":"matching"
             }
        #print(RunNumber)
        print(ii.replace(".json",".nii.gz"))
        print("will be saved as \n")
        print(get_bids_path(a))
        newfile = get_bids_path(a)
        #matchingfmaps.loc[matchingfmaps["filepath"] == ii, "newname"] = newfile
        bidsdir = parser["output"] / newfile.parent
        if not bidsdir.exists():
            bidsdir.mkdir()
        copy2(ii.replace(".json",".nii.gz"),parser["output"] / newfile)
        J = readjson(ii)
        J["B0FieldIdentifier"] = "Field1" + (f"ses{ses}" if ses is not None else "") # This will always use Field 1 # Need to manually adjust if you want something different
        writejson(J, str(parser["output"] / newfile).replace(".nii.gz", ".json"))
    runcount = 0
    RunNumber = 0
    for i,ii in enumerate(natsorted(oppositefmaps["filepath"])): # this will put the files in order of acquisition, making things easier.
        if not ii.__contains__('e1'): # The idea of these lines is to get the first echo only. If you want something different edit this
            continue
        if not ii.__contains__('DISTORTION'): # Likewise I'm discarding the single bands for simplicity. If you want you can change this
            continue
        RunNumbernew = int(ii.split("_")[-2].replace(".json",""))
        if (RunNumbernew > RunNumber) and (i > 1):
            RunNumber = RunNumbernew
            runcount = runcount + 1
        if runcount == 0:
            runcounter = None
        else :
            runcounter = runcount
        a = {"subject" :parser['id'] ,
             "session" : ses,
             "run" : runcounter,
             "datatype": "fmap",
             "suffix" : "epi",
             "extension":".nii.gz",
             "direction":"opposite"
             }
        #print(RunNumber)
        print(ii.replace(".json",".nii.gz"))
        print("will be saved as \n")
        print(get_bids_path(a))
        newfile = get_bids_path(a)
        #oppositefmaps.loc[oppositefmaps["filepath"] == ii, "newname"] = newfile
        bidsdir = parser["output"] / newfile.parent
        if not bidsdir.exists():
            bidsdir.mkdir()
        copy2(ii.replace(".json",".nii.gz"),parser["output"] / newfile)
        J = readjson(ii)
        J["B0FieldIdentifier"] = "Field1" + (f"ses{ses}" if ses is not None else "") # This will always use Field 1 # Need to manually adjust if you want something different
        writejson(J, str(parser["output"] / newfile).replace(".nii.gz", ".json"))    

    if not Path( parser["output"] / "dataset_description.json").exists():
        J =  {"Name": "MBME for TMS",
             "BIDSVersion": "1.8.0",
             "Authors": [""],
             "Acknowledgements": "This BIDS dataset was created using the scripts in https://github.com/zugmana/xnat-access created by Andre Zugman"
             }
        writejson(J, parser["output"] / "dataset_description.json" )
    # Clean up
    if Path( parser["output"] / parser["id"]).is_dir():
        rmtree(Path( parser["output"] / parser["id"]))
        
if __name__ == "__main__" :
    main()
         
