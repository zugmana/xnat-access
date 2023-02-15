#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep  4 16:57:35 2020

@author: winkleram
"""

import os
import sys
import argparse
import json
import pandas as pd
import numpy as np
import glob
from shutil import copy2 
from natsort import natsorted, ns
import nibabel as nib
import warnings
import datetime


def printHelp(argv, description): # ===========================================

    # Print help. This is meant to be called from parseArguments
    print(description)
    print("")
    print("Usage:")
    print("nidb2bids -n <dirin> -b <dirout>")
    print("")
    print("-n : Input NIDB directory, with .nii.gz and .json files.")
    print("-b : Output BIDS directory.")
    print("-r : Renumber entities as 1, 2, 3, etc.")
    print("")
    print("_____________________________________")
    print("Anderson M. Winkler, modified by Andre Zugman")
    print("National Institutes of Health")
    print("First version: Sep/2020")
    print("This version:  Feb/2023")
    print("Modified to use with xnat-access")
    print("")
    exit(1)
    
def parseArguments(argv): # ===================================================

    # Parse arguments
    description = "Convert the input NIDB directory into BIDS."
    if len (argv) <= 1:
        printHelp(argv, description)
    else:
        epilog = "Run without arguments for basic usage information."
        parser = argparse.ArgumentParser(description=description, epilog=epilog)
        parser.add_argument('-n', '--nidb',      help="Input NIDB directory.",
                            type=str, dest='dirin', action='store', required=False)
        parser.add_argument('-b', '--bids',      help="Output BIDS directory.",
                            type=str, dest='dirout', action='store', required=True)
        parser.add_argument('-r', '--renumber',  help="Re-number sessions, runs and echos to 1, 2, etc...",
                            dest='renumber', action='store_true', required=False)
        args = parser.parse_args(argv[1:])
        return args

def readjson(jsonfile): # =====================================================
    with open(jsonfile, 'r') as fp:
        J = json.load(fp)
    return J

def writejson(J, jsonfile): # =================================================
    with open(jsonfile, 'w') as fp:
        json.dump(J, fp, indent=2)
    return

def readhdr(niftifile): # =====================================================
    nii = nib.load(niftifile)
    dim = nii.header.get_data_shape()
    pixdim = nii.header.get_zooms()
    return dim, pixdim

# def link2copy(file): # ========================================================
#     # Replaces a hard link for an actual copy of the file
#     pth, nam = os.path.split(file)
#     tmpfile = os.path.join(pth, '{}.tmp'.format(nam))
#     copyfile(file, tmpfile)
#     os.remove(file)
#     os.rename(tmpfile, file)
#     return

def isbidsfile(filename): # ===================================================
    fnam, fext = os.path.splitext(filename)
    if fext == '.gz':
        fnam, fext = os.path.splitext(fnam)
        fext = fext + '.gz'
    if fext in ['.json', '.nii', '.nii.gz', '.bvec', '.bval', '.tsv']:
        isit = True
    else:
        isit = False
    fnam = os.path.basename(fnam)
    return isit, fnam, fext

def simplifystring(S):  # =====================================================
    special_chars = [' ', '-', '_', '.', '+', '(', ')', '/','\n']
    for c in special_chars:
        S = S.replace(c, '')
    S = S.lower()
    return S

def cleanentity(curdir, f, entity='run'): # ==================================
    # Drop redundant entities such as 'run' or 'echo', and rename
    # them to sequential numbers where they're not dropped.
    fnam, fext = os.path.splitext(f)
    if fext == '.gz':
        fnam, fext = os.path.splitext(fnam)
        fext = fext + '.gz'
    ftok = fnam.split('_')
    for idxt, t in enumerate(ftok):
        if t.startswith('{}-'.format(entity)):
            ftok[idxt] = '{}-*'.format(entity)
    flist = glob.glob(os.path.join(curdir, '_'.join(ftok) + fext))
    flist.sort()
    if isinstance(flist, list):
        if len(flist) == 1 and entity != 'ses': # note the exception for ses, as we always need a ses-1
            for idxt, t in enumerate(ftok):
                if t.startswith('{}-'.format(entity)):
                    ftok.pop(idxt)
        else:
            idxf = flist.index(os.path.join(curdir, fnam + fext)) + 1
            for idxt, t in enumerate(ftok):
                if t.startswith('{}-'.format(entity)):
                    ftok[idxt] = '{}-{}'.format(entity, idxf)
    newfnam = '_'.join(ftok)
    oldfile = os.path.join(curdir, fnam + fext)
    newfile = os.path.join(curdir, newfnam + fext)
    return oldfile, newfile

def swapfields(json): # =======================================================
    J = readjson(json)
    if J['Modality'] == 'MR' and J['Manufacturer'] == 'GE':
        tmp = J['SeriesDescription']
        J['SeriesDescription'] = J['ProtocolName']
        J['ProtocolName'] = tmp
        writejson(J, json)
    return

def findphysiotype(path):
    with open(path,'r') as file:
        lines = file.readlines()
        for l in lines:
            #print(l)
            if "Series De" in l:
                #print(l)
                x = l.split(":")[-1]
                x = simplifystring(x)
    return x
#%%
# =============================================================================
#   MAIN FUNCTION
# =============================================================================
def main() :
#%%
# Parse arguments
    if hasattr(sys, "ps1") :
        args={}
        dirin="/EDB/SDAN/temp/test02-14-2/nifti"
        dirout="/EDB/SDAN/temp/test02-14-2/BIDS"
        renumber = True
    else :       
        args = parseArguments(sys.argv)
        dirin = args.dirin
        dirout = args.dirout
        renumber = args.renumber
    # Note that dirin (-n nidbdir) is optional. If not supplied, and if the BIDS
    # directory exists and -r was supplied, it will renumber entities in the bidsdir.
    if dirin != None:
        # Get a mapping between NIDB UID and AltUIDs
        if not os.path.isdir(dirin) : 
            sys.exit('source dir does not exist')
        slist = next(os.walk(dirin))[1]
        slist.sort()
        
        if os.path.isdir(dirout):
            print('Error: Output directory already exists: {}'.format(dirout))
            #exit()
        else:
            os.makedirs(dirout)
        

        
        D = {}
        P = {}
        # For each file in the newly created BIDS dir:
        for curdir, subdirs, files in os.walk(dirin):
            # Get physio if available

            if "physio" in curdir :
                sdan_id = [i for i in curdir.split("/") if "sub-" in i][0]
                if sdan_id not in P:
                    P[sdan_id] = {}
                for f in natsorted(files, alg=ns.IGNORECASE):
                    if "1D" in f:
                        acquisition_date1   = f.split('_')[-2]
                        acquisition_date   = datetime.datetime.strptime(acquisition_date1,'%Y%m%d')
                        if acquisition_date not in P[sdan_id] :
                            P[sdan_id][acquisition_date] = pd.DataFrame(columns=[
                                                'seriesnum','acquisition_date','acquisition_time','newfnam','oldpath','datatype'])
                        seriesnum = int(f.split('_')[-3]) - 1
                        #print(seriesnum)
                        acquisition_time = f.split('_')[-1].replace(".1D","")
                        acquisition_time   = datetime.datetime.strptime("{}:{}".format(acquisition_date1, acquisition_time),'%Y%m%d:%H%M%S')
                        datatype = f.split('_')[0]
                        if datatype == "ECG" : 
                            P[sdan_id][acquisition_date].loc[f,'recordingstr'] = "_recording-cardiac" 
                        if datatype == "Resp" : 
                            P[sdan_id][acquisition_date].loc[f,'recordingstr'] = "_recording-respiratory"    
                        P[sdan_id][acquisition_date].loc[f,'datatype'] = datatype
                        P[sdan_id][acquisition_date].loc[f,'acquisition_time'] = acquisition_time
                        P[sdan_id][acquisition_date].loc[f,'acquisition_date'] = acquisition_date
                        P[sdan_id][acquisition_date].loc[f,'seriesnum'] = seriesnum
                        P[sdan_id][acquisition_date].loc[f,'oldpath'] =os.path.join(curdir,f)
                        matching = "_".join(f.split("_")[2:4])
                        matchingfile = [i for i in files if i.startswith(matching)][0]
                        P[sdan_id][acquisition_date].loc[f,'seriesdescription'] = findphysiotype(os.path.join(curdir,matchingfile))
                        
            else :
                
                # For each file in the current directory
                for f in natsorted(files, alg=ns.IGNORECASE):
                    print(f)
        #             # Check if the current file could belong to BIDS and if it's a JSON
                    isfbids, oldfnam, fext = isbidsfile(f)
                    if isfbids and fext == '.json':
    
                        
        #                 # The info on the filename is not really useful - will get from json:
                        sdan_id = oldfnam.split('_')[0]
                        
                        
                        J = readjson(os.path.join(curdir, f))
                        series_description = ''
                        acquisition_date   = curdir.split('/')[-2]
                        acquisition_date   = datetime.datetime.strptime(acquisition_date,'%m-%d-%Y')
                        acquisition_time   = ''
                        echo_time          = ''
                        serialnum          = ''
                        seriesnum          = ''
                        if sdan_id not in D:
                            D[sdan_id] = {}
                        if acquisition_date not in D[sdan_id] :
                            D[sdan_id][acquisition_date] = pd.DataFrame(columns=[
                                                        'seriesnum','serialnum','series_description',
                                                        'acquisition_date','acquisition_time','echo_time','newfnam','oldpath','datatype'])    
                        #print (J)
                        
                        if 'SeriesDescription' in J:
                            series_description = simplifystring(J['SeriesDescription'])
                            D[sdan_id][acquisition_date].loc[oldfnam,'series_description'] = series_description
                        if 'EchoTime' in J:
                            echo_time = '{0:.4f}'.format(J['EchoTime']).replace('0.', '')
                            D[sdan_id][acquisition_date].loc[oldfnam,'echo_time'] = echo_time
                        if 'ScanOptions' in J:
                            scan_options = J['ScanOptions']
                            D[sdan_id][acquisition_date].loc[oldfnam,'scan_options'] = scan_options
                        if 'SeriesNumber' in J:
                            seriesnum = J['SeriesNumber']
                            D[sdan_id][acquisition_date].loc[oldfnam,'seriesnum'] = seriesnum
                        if 'DeviceSerialNumber' in J:
                            serialnum = J['DeviceSerialNumber']
                            D[sdan_id][acquisition_date].loc[oldfnam,'serialnum'] = serialnum
                        #print(D[sdan_id][acquisition_date])
                        if 'AcquisitionTime' in J:                    
                            acquisition_time = datetime.datetime.strptime("{}T{}".format(acquisition_date.strftime('%m-%d-%Y'),J['AcquisitionTime']),
                                                                          '%m-%d-%YT%H:%M:%S.%f')
                            D[sdan_id][acquisition_date].loc[oldfnam,'acquisition_time'] = acquisition_time
                            #print(acquisition_time)
                        D[sdan_id][acquisition_date].loc[oldfnam,'acquisition_date'] = acquisition_date
                        D[sdan_id][acquisition_date].loc[oldfnam,'oldpath'] = curdir
        #                 # Read the NIFTI file and collect some information from the header
                        dim, pixdim = readhdr(os.path.join(curdir, f).replace('.json','.nii.gz'))
                        D[sdan_id][acquisition_date].loc[oldfnam,'dimi']    = dim[0]
                        D[sdan_id][acquisition_date].loc[oldfnam,'dimj']    = dim[1]
                        D[sdan_id][acquisition_date].loc[oldfnam,'dimk']    = dim[2]
                        D[sdan_id][acquisition_date].loc[oldfnam,'pixdimi'] = round(pixdim[0]*100)/100
                        D[sdan_id][acquisition_date].loc[oldfnam,'pixdimj'] = round(pixdim[1]*100)/100
                        D[sdan_id][acquisition_date].loc[oldfnam,'pixdimk'] = round(pixdim[2]*100)/100
                        #print(D)
                        
        #                 # Now prepare to rename according to the type of file
                        substr   = '{}'.format(sdan_id) # subject ID 
                        taskstr  = '' # string sto store the task name
                        datatype = '' # string to store the datatypeectory for this type of image
                        recstr   = '' # string to store the kind of reconstruction
                        dirstr   = '' # string to store the direction of phase enconding
                        echstr   = None # strong to store the echo time
                        modstr   = '' # string to store the type of modality
                        acqstr   = '' #string to store acquisition
                        has_fmap   = 'no'
                        multi_echo = False
                        #print(sesstr)
                        #print(runstr)
                        # ===== ANATOMY ===================================================
                        print('Simplified SeriesDescription: {}'.format(series_description))
                        if 'anatt1wmprage1mm' in series_description: # --------------------
                            # This is a T1w
                            datatype = 'anat'
                            modstr   = '_T1w'
                            if   'FILTERED_GEMS' in scan_options:
                                recstr = '_rec-norm'
                            else:
                                recstr = '_rec-orig'
                            has_fmap   = 'no'
                            multi_echo = False
                            
                        elif  'mprage'   in series_description or \
                            'anatomica'  in series_description or \
                            'fspgr'      in series_description or \
                            'bravo'      in series_description or \
                            'mprg'       == series_description: # -------------------------
                            # This is a T1w
                            datatype = 'anat'
                            modstr   = '_T1w'
                            has_fmap   = 'no'
                            multi_echo = False
                        
                        elif 't1map' == series_description: # -----------------------------
                            datatype = 'anat'
                            modstr = '_T1w'
                            has_fmap = 'no'
                            multi_echo = False
                        
                        elif series_description.startswith('sagittal') and \
                            'pd' in series_description: # ---------------------------------
                            # This is a Proton Density
                            datatype = 'anat'
                            modstr = '_PD'
                            has_fmap = 'no'
                            multi_echo = False
                            
                        elif 'sagwholebraint2frfse' in series_description: # --------------
                            # This is a T2w
                            datatype = 'anat'
                            modstr   = '_T2w'
                            if   'FILTERED_GEMS' in scan_options:
                                recstr = '_rec-norm'
                            else:
                                recstr = '_rec-orig'
                            has_fmap   = 'no'
                            multi_echo = False
                            
                        elif 't217mmfatsat' == series_description: # ----------------------
                            # This is a T2w
                            datatype = 'anat'
                            modstr = '_T2w'
                            has_fmap = 'no'
                            multi_echo = False
                            
                        elif 'anatt2wcube' in series_description: # -----------------------
                            # This is a T2w
                            datatype = 'anat'
                            modstr   = '_T2w'
                            if   'FILTERED_GEMS' in scan_options:
                                recstr = '_rec-norm'
                            else:
                                recstr = '_rec-orig'
                            has_fmap   = 'no'
                            multi_echo = False
                            
                        elif 'midaxt2flair1x1x4mm' in series_description: # ---------------
                            # This is a FLAIR
                            datatype = 'anat'
                            modstr   = '_FLAIR'
                            if   'FILTERED_GEMS' in scan_options:
                                recstr = '_rec-norm'
                            else:
                                recstr = '_rec-orig'
                            has_fmap   = 'no'
                            multi_echo = False
                        
                        elif 'flair' in series_description: # -----------------------------
                            # This is a FLAIR
                            datatype = 'anat'
                            modstr = '_FLAIR'
                            has_fmap = 'no'
                            multi_echo = False
                            
                        # ===== FIELDMAPS =================================================
                            
                        elif 'distortioncorrection' in series_description or \
                              'fordistortion'        in series_description or \
                              'oppositephaseencode'  in series_description or \
                              'blipforward'          in series_description or \
                              'blipreverse'          in series_description or \
                              'blipup'               in series_description or \
                              'blipdown'             in series_description or \
                              'matchingpe'           in series_description or \
                              'matchingblippe'       in series_description or \
                              'oppositeblippe'       in series_description or \
                              'oppositepe'           in series_description: # --------------
                            # These are distortion correction scans
                            datatype = 'fmap'
                            modstr   = '_epi'
                            if   'matching' in series_description or \
                                  'forward'  in series_description or \
                                  'blipup'   in series_description:
                                dirstr = '_dir-matching'
                            elif 'opposite' in series_description or \
                                  'reverse'  in series_description or \
                                  'blipdown' in series_description:
                                dirstr = '_dir-opposite'
                            has_fmap    = 'no'
                            multi_echo = False
                            if 'epi25iso' in series_description :
                                 multi_echo = True  
                        
                        # ===== TASKS =====================================================
                        
                        elif 'fmrienc'       == series_description or \
                              series_description.startswith('axialgreepirt') or \
                              series_description.startswith('saggreepirt')   or \
                              series_description.startswith('sagepirt')      or \
                              'tricomi'       in series_description or \
                              'knutson'       in series_description or \
                              'hfloc'         == series_description or \
                              'ebaloc'        == series_description: # ---------------------
                            # This is for unknown task protocols run in the past
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-unknown'
                            has_fmap   = 'no'
                            multi_echo = False
                        
                        elif 'morph' == series_description: # -----------------------------
                            # This is the "Morph" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-morph'
                            has_fmap   = 'no'
                            multi_echo = False
                            
                        elif 'dotprobe' == series_description: # --------------------------
                            # This is the "Dot Probe" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-DP'
                            has_fmap   = 'no'
                            multi_echo = False
                        
                        elif 'newdotprobeepi' == series_description: # --------------------
                            # This is the "New Dot Probe" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-newDP'
                            has_fmap   = 'no'
                            multi_echo = False
            
                        elif 'stanforddotprobe' in series_description: # ------------------
                            # This is the "Stanford Dot Probe" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-stanfordDP'
                            has_fmap   = 'no'
                            multi_echo = False
                       
                        elif 'ba8ba12' == series_description: # ---------------------------
                            # This is the "BA8BA-12" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-ba8ba12'
                            has_fmap   = 'no'
                            multi_echo = False
                        
                        elif 'facesaxialgreepirt1run' == series_description: # ------------
                            # This is the "Faces" task (pilot only)
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-faces'
                            has_fmap   = 'no'
                            multi_echo = False
                            
                        elif 'stop' in series_description and \
                            'greepirt8runs' in series_description: # ----------------------
                            # This is the "Stop" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-stop'
                            has_fmap   = 'no'
                            multi_echo = False
                        
                        elif 'newchatroomiirun' in series_description : # -----------------
                            # This is the "New Chatroom II" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-chatroom2'
                            has_fmap   = 'no'
                            multi_echo = False
                        
                        elif 'breathhold' == series_description : # -----------------------
                            # This is the "Breath-hold" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-breathhold'
                            has_fmap   = 'no'
                            multi_echo = False
                        
                        elif 'pd4runs' == series_description : # --------------------------
                            # This is the "PD" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-PD'
                            has_fmap   = 'no'
                            multi_echo = False
                        
                        elif 'affectivepriming' in series_description : # -----------------
                            # This is the "Affective Priming" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-AffectivePriming'
                            has_fmap   = 'no'
                            multi_echo = False
                            
                        elif series_description.startswith('foxdotprobe') : # -------------
                            # This is the "Affective Priming" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-foxDP'
                            has_fmap   = 'no'
                            multi_echo = False
                            
                        elif 'longtrial' in series_description: # -------------------------
                            # This is the "Fox Long Trial" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-foxlong'
                            has_fmap   = 'no'
                            multi_echo = False
                        
                        elif 'shorttrial' in series_description: # ------------------------
                            # This is the "Fox Short Trial" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-foxshort'
                            has_fmap   = 'no'
                            multi_echo = False
                        
                        elif 'parametricfaces4runs' == series_description: # --------------
                            # This is the "Parametric Faces" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-parafaces'
                            has_fmap   = 'no'
                            multi_echo = False
                            
                        elif 'context3runs' == series_description: # ----------------------
                            # This is the "Context" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-context'
                            has_fmap   = 'no'
                            multi_echo = False
                        
                        elif 'extinctionrecall2runs' == series_description or \
                              'extinctionrecall3runs' == series_description: # -------------
                            # This is the "Extinction Recall" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-ER'
                            has_fmap   = 'no'
                            multi_echo = False
                            
                        elif 'rooms3runs' == series_description: # ------------------------
                            # This is the "Rooms" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-rooms'
                            has_fmap   = 'no'
                            multi_echo = False
                        
                        elif 'facelocalizer' == series_description: # ---------------------
                            # This is the "Face localizer" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-facelocalizer'
                            has_fmap   = 'no'
                            multi_echo = False
                            
                        elif 'motorlocalizer' == series_description: # --------------------
                            # This is the "Motor localizer" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-motorlocalizer'
                            has_fmap   = 'no'
                            multi_echo = False
                        
                        elif series_description.startswith('facerealtime'): # -------------
                            # This is the "Face realtime" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-facerealtime'
                            has_fmap   = 'no'
                            multi_echo = False
                            
                        elif series_description.startswith('motorrealtime'): # ------------
                            # This is the "Motor realtime" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-motorrealtime'
                            has_fmap   = 'no'
                            multi_echo = False
                        
                        elif 'weissman6runs' in series_description: # ---------------------
                            # This is the "Weissman" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-weissman'
                            has_fmap   = 'no'
                            multi_echo = False
                        
                        elif 'foxtrial4runs' in series_description: # ---------------------
                            # This is the "Fox trial" task (pilot?)
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-foxtrial'
                            has_fmap   = 'no'
                            multi_echo = False
                        
                        elif 'affectiveposnergame3run' in series_description: # -----------
                            # This is the "Affective Posner Game" task (pilot?)
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-affposner'
                            has_fmap   = 'no'
                            multi_echo = False
                            
                        elif 'conflictadaptation2runs' in series_description: # -----------
                            # This is the "Conflict Adaptation" task (pilot?)
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-conflictadapt'
                            has_fmap   = 'no'
                            multi_echo = False
                            
                        elif series_description.startswith('amir'): # ---------------------
                            # This is the "Amir" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-amir'
                            has_fmap   = 'no'
                            multi_echo = False
                            
                        elif 'predictionerrors' in series_description: # ------------------
                            # This is the "Prediction Error" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-amir'
                            has_fmap   = 'no'
                            multi_echo = False
                            
                        elif 'gender3runs182reps' in series_description: # ----------------
                            # This is the "Gender" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-gender'
                            has_fmap   = 'no'
                            multi_echo = False
                        
                        elif 'pinata6runs' in series_description: # -----------------------
                            # This is the "Pinata" task (note 2 versions in the database)
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-pinata'
                            has_fmap   = 'no'
                            multi_echo = False
                        
                        elif 'littlefoxdp4runs' in series_description: # ------------------
                            # This is the "Little Fox Dot-Probe" task (note multiple versions)
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-lfDP'
                            has_fmap   = 'no'
                            multi_echo = False
            
                        elif 'littlefoxdp24runs111reps' in series_description: # ----------
                            # This is the "Little Fox Dot-Probe 2" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-lfDP2'
                            has_fmap   = 'after'
                            multi_echo = False
                            
                        elif 'littledp34runs111reps' == series_description or \
                            'littlefoxdp34runs111reps' == series_description: # -----------
                            # This is the "Little Fox Dot-Probe 3" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-lfDP3'
                            has_fmap   = 'after'
                            multi_echo = False
                        
                        elif 'labeling4runs226reps' in series_description: # --------------
                            # This is the "Labeling" task (note multiple versions)
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-labeling'
                            has_fmap   = 'no'
                            multi_echo = False
                        
                        elif 'recall23runs272reps' in series_description: # ---------------
                            # This is the "Extinction Recall 2" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-ER2'
                            has_fmap   = 'no'
                            multi_echo = False
                        
                        elif 'sagflanker28004runs' == series_description or \
                            'flanker4runs' == series_description: # -----------------------
                            # This is the "Flanker" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-flanker'
                            has_fmap   = 'no'
                            multi_echo = False
                        
                        elif '1mintriggertest' == series_description: # -------------------
                            # This is the "Trigger test"
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-trigger'
                            has_fmap   = 'no'
                            multi_echo = False
                            
                        elif 'affpos2runs' == series_description: # -----------------------
                            # This is the "Affective Posner" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-AP'
                            has_fmap   = 'no'
                            multi_echo = False
                        
                        elif 'tau2runs' == series_description: # --------------------------
                            # This is the TAU task (first protocol)
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-TAU'
                            has_fmap   = 'no'
                            multi_echo = False
                        
                        elif series_description.startswith('virtualschool4runs') and \
                            series_description.endswith('reps'): # ------------------------
                            # This is the "Virtual School" task (first version)
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-virtualschool'
                            has_fmap   = 'no'
                            multi_echo = False
                        
                        elif 'er2bells2runs343reps' == series_description: # --------------
                            # This is the "Extinction Recall/Bells" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-bells'
                            has_fmap   = 'no'
                            multi_echo = False
                            
                        elif 'signaldropoutevaluation' == series_description: # -----------
                            # This is the "Signal dropout evaluation" series
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-dropout'
                            has_fmap   = 'no'
                            multi_echo = False
                            
                        elif 'flankerarrow4runs' == series_description: # -----------------
                            # This is the "Flanker Arrow" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-flankerarrow'
                            has_fmap   = 'no'
                            multi_echo = False
                        
                        elif 'anticipationwi3runs' == series_description: # ---------------
                            # This is the "Anticipation (Wisconsin)" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-anticipation'
                            has_fmap   = 'no'
                            multi_echo = False
                            
                        elif 'flankerv24runs' in series_description: # --------------------
                            # This is the "Flanker 2" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-flanker2'
                            has_fmap   = 'no'
                            multi_echo = False
                           
                        elif 'virtualschoolv23runs' == series_description or \
                            'virtualschoolrepeat3runs' == series_description: # -----------
                            # This is the "Virtual School v2" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-virtualschool2'
                            has_fmap   = 'no'
                            multi_echo = False
                            
                        elif 'intbias3runs236reps' in series_description: # ---------------
                            # This is the Interaction Bias" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-intbias'
                            has_fmap   = 'no'
                            multi_echo = False
                            
                        elif 'labelingii4runs226reps' in series_description: # ------------
                            # This is the "Labeling II" task (note multiple versions)
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-labeling2'
                            has_fmap   = 'no'
                            multi_echo = False
                            
                        elif 'thinslices' == series_description or \
                            'thinslices4runs' == series_description: # --------------------
                            # This is the "Thin slices" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-thinslices'
                            has_fmap   = 'after'
                            multi_echo = False
                            
                        elif 'changetask6runs165reps' == series_description: # ------------
                            # This is the "Change" task (note multiple versions)
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-change'
                            has_fmap   = 'after'
                            multi_echo = False
                            
                        elif 'visualsearch' in series_description: # ----------------------
                            # This is the "Visual Search" task
                            if 'visualsearchx5runs' in series_description:
                                datatype   = 'func'
                                modstr     = '_bold'
                                taskstr    = '_task-visualsearch'
                                has_fmap   = 'no'
                                multi_echo = False
                            elif 'visualsearch3runs' in series_description:
                                datatype   = 'func'
                                modstr     = '_bold'
                                taskstr    = '_task-visualsearch'
                                has_fmap   = 'after'
                                multi_echo = False
                            
                        elif 'tau2runsfmrifepi3mm' in series_description: # ---------------
                            # This is the TAU2 task (new protocol)
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-TAU2'
                            has_fmap   = 'after'
                            multi_echo = False
                            
                        elif 'mid11run' in series_description: # --------------------------
                            # This is the "Monetary Incentive Delay" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-MID1'
                            has_fmap   = 'after'
                            multi_echo = False # Check this
                            
                        elif series_description.startswith('shapes'): # -------------------
                            # This is the "Shapes" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            has_fmap   = 'after'
                            multi_echo = False
                            if   'firstrun196reps'  in series_description:
                                taskstr = '_task-shapes1'
                            elif 'secondrun214reps' in series_description:
                                taskstr = '_task-shapes2'
                            elif 'thirdrun214reps'  in series_description:
                                taskstr = '_task-shapes3'
                            elif 'fourthrun136reps' in series_description:
                                taskstr = '_task-shapes4'
                            elif 'fifthrun196reps'  in series_description:
                                taskstr = '_task-shapes5'
                            
                        elif 'flankerv34runs' in series_description: # --------------------
                            # This is the "Flanker 3" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-flanker3'
                            has_fmap   = 'after'
                            multi_echo = False
            
                        elif 'epitaskap32runs183reps' in series_description: # ------------
                            # This is the "Affective Posner 3" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-AP3'
                            has_fmap   = 'after'
                            multi_echo = True
                        
                        elif 'socialflanker4runs' in series_description: # ----------------
                            # This is the "Social Flanker" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-socialflanker'
                            has_fmap   = 'after'
                            multi_echo = False
                            
                        elif 'taskchange25runs183reps' == series_description: # -----------
                            # This is the "Change 2" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-change2'
                            has_fmap   = 'after'
                            multi_echo = True
                            
                        elif 'prr3runs' == series_description or \
                            'prrtaskfinal1run' == series_description: # -------------------
                            # This is the PRR task (note multiple versions)
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-PRR'
                            has_fmap   = 'after'
                            multi_echo = False
                            
                        elif 'prrtask6runs' in series_description: # ----------------------
                            # This is the PRR task  (note multiple versions)
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-PRR'
                            has_fmap   = 'before'
                            multi_echo = False
                        
                        elif 'taskdwell' in series_description: # ----------------------
                            # This is the PRR task  (note multiple versions)
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-dwell'
                            has_fmap   = 'before'
                            multi_echo = False
                            
                        elif 'thepresent1run' in series_description: # --------------------
                            # This is the "The Present" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-thepresent'
                            has_fmap   = 'after'
                            multi_echo = False
                            
                        elif series_description.startswith('mmi'): # ----------------------
                            # This is the "MMI" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-MMI'
                            has_fmap   = 'before'
                            multi_echo = False
            
                        elif 'carnivalgame' in series_description: # ----------------------
                            # This is the "Carnival" task
                            datatype = 'func'
                            modstr   = '_bold'
                            if   'game1' in series_description: 
                                taskstr = '_task-carnival1'
                            elif 'game2' in series_description: 
                                taskstr = '_task-carnival2'
                            elif 'game3' in series_description: 
                                taskstr = '_task-carnival3'
                            has_fmap   = 'before'
                            multi_echo = False
                        
                        elif 'francis' in series_description: # ---------------------------
                            # This is Francis task (movie) and the associated rest
                            datatype   = 'func'
                            modstr     = '_bold'
                            if 'metaskfrancisepi38mmisome' in series_description or \
                                'francistaskepi38mmisome' in series_description: 
                                taskstr  = '_task-francis'
                                has_fmap = 'after'
                            elif 'merestfrancisepi38mmisome' in series_description or \
                                'francisrestingepi38mmisome' in series_description:
                                taskstr  = '_task-restpre'
                                has_fmap = 'after'
                            elif 'francispostrestingepi38mmisome' in series_description:
                                taskstr  = '_task-restpost'
                                has_fmap = 'before'
                            multi_echo = True
                        
                        elif 'epitaskprofile2runs165reps' in series_description: # --------
                            # This is the "Profile" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-profile'
                            has_fmap   = 'before'
                            multi_echo = False
                        
                        elif 'er33runs' in series_description: # --------------------------
                            # This is the "Extinction Recall 3" task
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-ER3'
                            has_fmap   = 'after'
                            multi_echo = False
            
                        elif series_description.startswith('epitask'): # ------------------
                            # These are the 3 tasks of the Wisconsin project
                            datatype   = 'func'
                            modstr     = '_bold'
                            has_fmap   = 'before'
                            multi_echo = False
                            if   'epitask1' == series_description:
                                taskstr = '_task-task1'
                            elif 'epitask2' == series_description:
                                taskstr = '_task-task2'
                            elif 'epitask3' == series_description:
                                taskstr = '_task-task3'
            
                        # ===== RESTING STATE =============================================
                        
                        elif 'restingme1run' == series_description: # ---------------------
                            # This is resting state FMRI, ME
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-rest'
                            has_fmap   = 'no'
                            multi_echo = True
                        
                        elif 'fmrifepi38mmisome' == series_description or \
                            'ap3restingepi38mmisome' == series_description or \
                            'axialmerestepi3mmiso' in series_description or \
                            'change2restingepi38mmisome' == series_description or \
                            'restingepi38mmisome' == series_description: # -----
                            # This is resting multi-echo
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-rest'
                            has_fmap   = 'after'
                            multi_echo = True
                        
                        elif series_description.startswith('epirest'): # ------------------
                            # These are the 2 resting state of the Wisconsin project
                            datatype   = 'func'
                            modstr     = '_bold'
                            has_fmap   = 'before'
                            multi_echo = False
                            if   'epiresting1' == series_description:
                                taskstr = '_task-rest1'
                            elif 'epiresting2' == series_description:
                                taskstr = '_task-rest2'
                        
                        elif 'resting' == series_description or \
                              'resting1run' == series_description: # -----------------------
                            # This is resting state FMRI 
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-rest'
                            has_fmap   = 'no'
                            multi_echo = False
                        
                        elif 'rest' in series_description and \
                              not 'distortioncorrection' in series_description: # ---------
                            # This is resting state FMRI 
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-rest'
                            has_fmap   = 'after'
                            multi_echo = False
                            
                        elif 'fmrifepi25isombme' == series_description or\
                            'fmrifepi2x2x2mmmbme' == series_description and \
                              not 'distortioncorrection' in series_description: # -----
                            # This is resting multi-echo
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-rest'
                            has_fmap   = 'before'
                            acqstr     = '_acq-MBME'
                            multi_echo = True
    
                        elif 'fmrifepi18x18x20mmmb' == series_description and \
                              not 'distortioncorrection' in series_description: # -----
                            # This is resting multi-echo
                            datatype   = 'func'
                            modstr     = '_bold'
                            taskstr    = '_task-rest'
                            has_fmap   = 'before'
                            acqstr     = '_acq-MBSE'
                            multi_echo = False 
                        
                        # ===== DIFFUSION =================================================
                        
                        elif series_description.startswith('edticdiflist') or \
                              series_description.startswith('edtissgrcdif') or \
                              series_description == 'uwdti': # -----------------------------
                            # This is diffusion (note multiple versions)
                            datatype   = 'dwi'
                            modstr     = '_dwi'
                            taskstr    = ''
                            has_fmap   = 'no'
                            multi_echo = False
                        
                        elif series_description.startswith('edti25mm62vol'): # ------------
                            # This is diffusion (note multiple versions)
                            datatype   = 'dwi'
                            modstr     = '_dwi'
                            has_fmap   = 'no'
                            multi_echo = False
                            if   series_description.endswith('ap'):
                                taskstr    = '_dir-AP'
                            elif series_description.endswith('pa'):
                                taskstr    = '_dir-PA'
                            else:
                                taskstr    = ''
                        
                        else: # -----------------------------------------------------------
                            print('Skipping: {}{} ({})'.format(oldfnam, fext, series_description))
                            datatype   = 'unknown'
                            modstr     = '_unknown'
                            taskstr    = ''
                            has_fmap   = 'no'
                            multi_echo = False
            
                        
                        # For multi-echo data, get the echo time
                        if datatype == 'func' and multi_echo and echo_time != '':
                            echstr = '_echo-{}'.format(echo_time)
                        # Messy fmap that might also have multiple echos This will need to be cleaned later manually.
                        if datatype == 'fmap' and multi_echo and echo_time != '':
                            echstr = '_echo-{}'.format(echo_time)
                        
                        #newfnam = '{}{}{}{}{}{}{}{}'.format(substr, sesstr, taskstr, 
                        #recstr, dirstr, runstr, echstr, modstr)
                        D[sdan_id][acquisition_date].loc[oldfnam,'datatype'] = datatype
                        D[sdan_id][acquisition_date].loc[oldfnam,'has_fmap'] = has_fmap
                        D[sdan_id][acquisition_date].loc[oldfnam,'substr'] = substr
                        #D[sdan_id][acquisition_date].loc[oldfnam,'sesstr'] = sesstr
                        D[sdan_id][acquisition_date].loc[oldfnam, 'taskstr'] = taskstr
                        D[sdan_id][acquisition_date].loc[oldfnam,'recstr'] = recstr
                        D[sdan_id][acquisition_date].loc[oldfnam,'dirstr'] = dirstr
                        D[sdan_id][acquisition_date].loc[oldfnam,'echstr'] = echstr
                        D[sdan_id][acquisition_date].loc[oldfnam,'modstr'] = modstr
                        D[sdan_id][acquisition_date].loc[oldfnam,'acqstr'] = acqstr
                        

        if renumber :
            for sdan_id in D :
                D[sdan_id] = dict((sorted(D[sdan_id].items(), reverse=False)))
                countses = 1
                for acquisition_date in D[sdan_id]:
                    #print(acquisition_date)
                    D[sdan_id][acquisition_date]["run"] = D[sdan_id][acquisition_date].sort_values(
                         "acquisition_time",ascending=True).groupby(["taskstr","recstr","dirstr","modstr","acqstr"]).cumcount()+1
                    D[sdan_id][acquisition_date]["ses"] = countses
                     #renumberecho
                    if len(D[sdan_id][acquisition_date].loc[~D[sdan_id][acquisition_date]["echstr"].isnull()]) > 0 :
                         echos = D[sdan_id][acquisition_date].loc[~D[sdan_id][acquisition_date]["echstr"].isnull()].sort_values(
                                 "echstr",ascending=True).groupby(["series_description","seriesnum"]).cumcount()+1
                         D[sdan_id][acquisition_date].loc[~D[sdan_id][acquisition_date]["echstr"].isnull(),"echstr"] = "_echo-" + echos.astype(str)
                         D[sdan_id][acquisition_date].loc[~D[sdan_id][acquisition_date]["echstr"].isnull(),"run"] = \
                             D[sdan_id][acquisition_date].loc[~D[sdan_id][acquisition_date]["echstr"].isnull()].sort_values(
                                  "acquisition_time",ascending=True).groupby(["seriesnum"]).ngroup()+1
                    D[sdan_id][acquisition_date].loc[D[sdan_id][acquisition_date]["echstr"].isnull(),"echstr"] = ''
                    D[sdan_id][acquisition_date]["newfnam"] = D[sdan_id][acquisition_date]["substr"] + '_ses-' + D[sdan_id][acquisition_date]["ses"].astype(str) \
                         + D[sdan_id][acquisition_date]["taskstr"] + D[sdan_id][acquisition_date]["acqstr"] + D[sdan_id][acquisition_date]["recstr"] + D[sdan_id][acquisition_date]["dirstr"] \
                             + '_run-' + D[sdan_id][acquisition_date]["run"].astype(str) + D[sdan_id][acquisition_date]["echstr"] \
                                + D[sdan_id][acquisition_date]["modstr"]
                    
                    countses = countses + 1
#                  
        else :
            for sdan_id in D :
                D[sdan_id] = dict((sorted(D[sdan_id].items(), reverse=False)))
                countses = 1
                for acquisition_date in D[sdan_id]:
                    D[sdan_id][acquisition_date]["ses"] = pd.to_datetime(D[sdan_id][acquisition_date]["acquisition_time"]).dt.strftime(
                        '%Y-%m-%d')
                    D[sdan_id][acquisition_date]["run"] = pd.to_datetime(D[sdan_id][acquisition_date]["acquisition_time"]).dt.strftime(
                        '%H-%M')
                    D[sdan_id][acquisition_date]["newfnam"] = D[sdan_id][acquisition_date]["substr"] + '_ses-' + D[sdan_id][acquisition_date]["ses"].astype(str) \
                         + D[sdan_id][acquisition_date]["taskstr"] + D[sdan_id][acquisition_date]["recstr"] + D[sdan_id][acquisition_date]["dirstr"] \
                             + '_run-' + D[sdan_id][acquisition_date]["run"].astype(str) + D[sdan_id][acquisition_date]["echstr"] \
                                + D[sdan_id][acquisition_date]["modstr"]
          
          
        # Deal with the rare cases of identical new filenames (e.g. mag/phase images from old fieldmaps)
        for sdan_id in D:
            for acquisition_date in D[sdan_id]:
                indices = D[sdan_id][acquisition_date].index.tolist()
                newfname_orig = D[sdan_id][acquisition_date]['newfnam'].copy()
                mi = 1
                for oldfnam in indices:
                    ma = sum(newfname_orig == D[sdan_id][acquisition_date].loc[oldfnam,'newfnam'])
                    if ma > 1:
                        tok = D[sdan_id][acquisition_date].loc[oldfnam,'newfnam'].split('_')
                        for t in tok:
                            if t.startswith('run-'):
                                D[sdan_id][acquisition_date].loc[oldfnam,'newfnam'] = D[sdan_id][acquisition_date].loc[oldfnam,'newfnam'].replace(t, '{}x{}'.format(t, mi))
                        mi = mi + 1
                    if mi == ma + 1:
                        mi = 1
        for sdan_id in D:
            #print(sdan_id)
            for acquisition_date in D[sdan_id]:
                #print(acquisition_date)
                #sesdf = D[sdan_id][acquisition_date]
                #print(sesdf)
                for oldfnam in D[sdan_id][acquisition_date].index:
                    #print(oldfnam)
                    oldpath = D[sdan_id][acquisition_date].loc[oldfnam,"oldpath"]
                    for iext in ['.json', '.nii.gz', '.bvec', '.bval', '.tsv']:
                        oldfile = os.path.join(oldpath, '{}{}'.format(oldfnam, iext))
                        #print(oldfile)
                        if os.path.isfile(oldfile):
                            newfile = os.path.join(dirout,
                                                   '{}'.format(sdan_id),
                                                   'ses-{}'.format(
                                                       D[sdan_id][acquisition_date].loc[oldfnam, 'ses']),
                                                   D[sdan_id][acquisition_date].loc[oldfnam,
                                                                                    'datatype'],
                                                   '{}{}'.format(D[sdan_id][acquisition_date].loc[oldfnam, 'newfnam'], iext))
                            newdir, newname = os.path.split(newfile)
                            if not os.path.isdir(newdir):
                                os.makedirs(newdir)
                            if (iext == '.json') and D[sdan_id][acquisition_date].loc[oldfnam,
                                                             'datatype'] == 'func':
                                print('checking if need to add task name to json')
                                
                                J = readjson(oldfile)
                                if 'TaskName' in J:
                                    print("TaskName in json")
                                    print('Moving: {} -> {}'.format(oldfile, newfile))
                                    copy2(oldfile, newfile)
                                else :
                                    J['TaskName'] = D[sdan_id][acquisition_date].loc[oldfnam,
                                                                     'taskstr'].replace("_task-","")
                                    print('Moving: {} -> {}'.format(oldfile, newfile))
                                    writejson(J, newfile)
                            else:
                                print('Moving: {} -> {}'.format(oldfile, newfile))
                                copy2(oldfile, newfile)
    

        for sdan_id in D:
            for acquisition_date in D[sdan_id]:
                df = D[sdan_id][acquisition_date]
                D[sdan_id][acquisition_date]["intendedfor"] = np.empty((len(D[sdan_id][acquisition_date]),0)).tolist()
                df = D[sdan_id][acquisition_date]
                if len(df.loc[(df["dirstr"] == "_dir-matching") | (df["dirstr"] == "_dir-opposite")]) > 0:
                    B0 = 0
                    for g, group in df.loc[df["modstr"] == "_epi"].groupby(["dirstr","run"], sort=False):
                        #print(group.index)
                        if 'opposite' in g[0]:
                            B0 = B0 + 1
                            D[sdan_id][acquisition_date].loc[group.index,"B0-identifier"] = "Field-{}".format(B0)
                            seriesnum = group['seriesnum'].unique()[0]
                            # Look for matching pair in +1 or -1 run. Sometimes there is just one opposite. This is fine
                            matchings = df.loc[(df["modstr"] == "_epi") &\
                                               ((df["seriesnum"] == seriesnum-1) | (df["seriesnum"] == seriesnum+1))&\
                                               (df["dirstr"] == "_dir-matching")    ]
                            #print(matchings.index)
                            D[sdan_id][acquisition_date].loc[matchings.index,"B0-identifier"] = "Field-{}".format(B0)
                    for i,ii in df.loc[df["has_fmap"]=="before"].iterrows() :
                        #print(i)
                        #print(ii)
                        seriesnum = ii["seriesnum"]
                        #find the first fmap before this run.
                        fmap_opposite = df.loc[(df["dirstr"] == "_dir-opposite") & (df["seriesnum"] < seriesnum)].iloc[-1]["B0-identifier"]
                        D[sdan_id][acquisition_date].loc[i,"B0-identifier"] = fmap_opposite
                        intendedfor = os.path.join('bids::',sdan_id,'ses-{}'.format(D[sdan_id][acquisition_date].loc[i,'ses']), 
                                                                     D[sdan_id][acquisition_date].loc[i,'datatype'], 
                                                                     '{}.nii.gz'.format(D[sdan_id][acquisition_date].loc[i,'newfnam']))
                        #have to for loop for some reason
                        for j in D[sdan_id][acquisition_date].loc[(D[sdan_id][acquisition_date]["B0-identifier"] == fmap_opposite) &\
                                                                  ((D[sdan_id][acquisition_date]["dirstr"] == "_dir-matching") |\
                                                                   (D[sdan_id][acquisition_date]["dirstr"] == "_dir-opposite"))].index :
                           #print(j)
                           #print(D[sdan_id][acquisition_date].loc[j,"intendedfor"] + intendedfor)
                           D[sdan_id][acquisition_date].loc[j,"intendedfor"].insert(-1,intendedfor)
                           #print(D[sdan_id][acquisition_date].loc[j,"intendedfor"])
                    #Now for after
                    for i,ii in df.loc[df["has_fmap"]=="after"].iterrows() :
                        #print(i)
                        #print(ii)
                        seriesnum = ii["seriesnum"]
                        #find the first fmap before this run.
                        fmap_opposite = df.loc[(df["dirstr"] == "_dir-opposite") & (df["seriesnum"] > seriesnum)].iloc[0]["B0-identifier"]
                        D[sdan_id][acquisition_date].loc[i,"B0-identifier"] = fmap_opposite
                        intendedfor = os.path.join('bids::',sdan_id,'ses-{}'.format(D[sdan_id][acquisition_date].loc[i,'ses']), 
                                                                     D[sdan_id][acquisition_date].loc[i,'datatype'], 
                                                                     '{}.nii.gz'.format(D[sdan_id][acquisition_date].loc[i,'newfnam']))
                        #have to for loop for some reason
                        for j in D[sdan_id][acquisition_date].loc[(D[sdan_id][acquisition_date]["B0-identifier"] == fmap_opposite) &\
                                                                  ((D[sdan_id][acquisition_date]["dirstr"] == "_dir-matching") |\
                                                                   (D[sdan_id][acquisition_date]["dirstr"] == "_dir-opposite"))].index :
                           #print(j)
                           #print(D[sdan_id][acquisition_date].loc[j,"intendedfor"] + intendedfor)
                           D[sdan_id][acquisition_date].loc[j,"intendedfor"].insert(-1,intendedfor)
                           #print(D[sdan_id][acquisition_date].loc[j,"intendedfor"])
                    
                    #Finally write to jsons.
                    for i,ii in D[sdan_id][acquisition_date].loc[D[sdan_id][acquisition_date]["datatype"] == "fmap"].iterrows() :
                        #print(i)
                        #print(ii)
                        jsonfile = os.path.join(dirout, 
                                                '{}'.format(sdan_id),
                                                'ses-{}'.format(ii['ses']),
                                                ii['datatype'],
                                                '{}.json'.format(ii['newfnam']))
                        J = readjson(jsonfile)
                        if 'IntendedFor' in J:
                            print("Intended for already in the Json?")
                        else :
                            J['IntendedFor'] = ii["intendedfor"]
                            J["B0FieldIdentifier"] = ii["B0-identifier"]
                        
                        writejson(J, jsonfile)
                   # Write Jsons for functional too
                    for i,ii in D[sdan_id][acquisition_date].loc[((D[sdan_id][acquisition_date]["datatype"] == "func") &\
                                                                 ((D[sdan_id][acquisition_date]["has_fmap"] == "before") |\
                                                                 (D[sdan_id][acquisition_date]["has_fmap"] == "after"))) ].iterrows() :
                        
                        jsonfile = os.path.join(dirout, 
                                                '{}'.format(sdan_id),
                                                'ses-{}'.format(ii['ses']),
                                                ii['datatype'],
                                                '{}.json'.format(ii['newfnam']))
                        J = readjson(jsonfile)
                        if 'B0FieldIdentifier' in J:
                            print("B0Field for already in the Json?")
                        else :
                            #J['IntendedFor'] = ii["intendedfor"]
                            J["B0FieldSource"] = ii["B0-identifier"]
                        writejson(J, jsonfile)
# At last Match Physio
        for sdan_id in P:
            for acquisition_date in P[sdan_id]:
                df = P[sdan_id][acquisition_date].sort_values("seriesnum")
                
                for g, group in df.groupby(["datatype","seriesdescription"], sort=False):
                    #print(g)
                    #print(group)
                    matches = D[sdan_id][acquisition_date].loc[
                        D[sdan_id][acquisition_date]["series_description"] == g[1]]
                    matches = matches.drop_duplicates(subset="seriesnum")
                    #print(matches)
                    if len(matches) == len(group):
                        print("matching physio to runs")
                        group["newfnam"] = matches["newfnam"].values
                        group["newfnam"] = group["newfnam"].str.replace("_echo-1","")
                        group["newfnam"] = group["newfnam"].str.replace("_bold","_physio.tsv")
                        group["ses"] = matches["ses"].values
                        group["datatype"] = matches["datatype"].values
                        group["difftime"] = matches["acquisition_time"].values - group["acquisition_time"]
                        #print(group["newfnam"])
                        for i,ii in group.iterrows():
                            oldfile = ii["oldpath"]
                            newfile = os.path.join(dirout,
                                                   '{}'.format(sdan_id),
                                                   'ses-{}'.format(
                                                       ii.loc['ses']),
                                                   ii.loc['datatype'],
                                                   '{}_physio.tsv'.format(ii.loc['newfnam'].replace("_physio.tsv", ii["recordingstr"])))
                            print("Moving {} -> {}".format(oldfile,newfile))
                            copy2(oldfile, newfile)
                            J = {}
                            if "Resp" in i :
                                J = {"SamplingFrequency": 50.0,
                                     "StartTime": ii["difftime"].total_seconds(),
                                     "Columns": ["respiratory"]}
                            elif "ECG" in i :
                                J = {"SamplingFrequency": 50.0,
                                     "StartTime": ii["difftime"].total_seconds(),
                                     "Columns": ["cardiac"]}
                            jsonfile = newfile.replace("tsv","json")
                            writejson(J, jsonfile)
                    else :
                        print("could not match physio to run. You'll have to do it manually.")
    #Create dataset_description
    if not os.path.exists(os.path.join(dirout,"dataset_description.json")):
        J =  {"Name": dirout,
             "BIDSVersion": "1.8.0",
             "Authors": [""],
             "Acknowledgements": "This BIDS dataset was created using the scripts in https://github.com/zugmana/xnat-access created by Andre Zugman"
             }
        writejson(J, os.path.join(dirout,"dataset_description.json"))
    #%%
if __name__ == '__main__':
    main()