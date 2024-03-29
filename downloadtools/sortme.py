#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 15 13:58:08 2018

@author: winkleram
"""

import os, sys, numpy, pydicom, json, datetime , shutil

def printHelp(argv): # ========================================================
    # Print help
    print("Sorts DICOM files from a multi-echo enabled version of GE's EPI sequence.")
    print("The algorithm for sorting is based on a set of tags found on DICOM headers")
    print("and on a shell script, both of which were provided by Wen-Ming Luh.")
    print("")
    print("Usage:")
    print(argv[0] + " <dicomdir> [file extension] [isSorted]")
    print("")
    print("Inputs:")
    print("- dicomdir       : Directory containing DICOM files.")
    print("                   Default: current directory.")
    print("- file_extension : File extension of files in the directory.")
    print("                   Default: dcm")
    print("- isSorted?      : A True/False to indicate whether multi-echo")
    print("                   have been sorted by an earlier run of this")
    print("                   program. Default: False")
    print("")
    print("Files are reorganized (moved) into echo_####.")
    print("")
    print("This script requires:")
    print(" - PyDICOM (https://github.com/pydicom/pydicom)")
    print(" - Chris Rorden's dcm2niix")
    print("")
    print("Originally written by Vinai Roopchansingh (NIH/NIMH/FMRIF) on 2013.01.31")
    print("Updated to use PyDICOM by Vinai Roopchansingh on 2018.03.26.")
    print("This version adapted to EDB by Anderson Winkler (NIH/NIMH/BSD) on 2018.08.09.")
    print("This version adapted to EDB usage with XNAT by Andre Zugman (NIH/NIMH/BSD) on 2022.10.27.")
    print("")
    print("_____________________________________")
    print("Anderson M. Winkler")
    print("National Institutes of Health (NIH/NIMH)")
    print("Aug/2018")
    print("http://brainder.org")

def processOptions(argv): # ===================================================
    # Parse arguments
    # Defaults ----------------------------------------------------------------
    workDir  = "."
    fileExt  = "dcm"
    isSorted = False
    # -------------------------------------------------------------------------
    if(len(argv) <= 1):
       printHelp(argv)
       sys.exit(0)
    workDir = argv[1]
    if len(argv) > 2:
       fileExt = argv[2]
    if len(argv) > 3:
        if argv[3].lower() == 'true':
            isSorted = True
    return(workDir, fileExt, isSorted)

def updateProgress(progress): # ===============================================
    barLength = 50
    status = ""
    if isinstance(progress, int):
        progress = float(progress)
    if not isinstance(progress, float):
        progress = 0
        status = "Error: progress var must be float.\r\n"
    if progress < 0:
        progress = 0
        status = "Halt...\r\n"
    if progress >= 1:
        progress = 1
        status = "Done.\r\n"
    block = int(round(barLength*progress))
    text = "\rPercent: [{0}] {1}% {2}".format( "#"*block + "-"*(barLength-block), round(progress*100), status)
    sys.stdout.write(text)
    sys.stdout.flush()

def returnTagValue(x, tag): # =================================================
    # Return the content of a tag of a DICOM file, or of a previously
    # loaded DICOM header.
    if   isinstance(x, str):
        dicomHdr = pydicom.read_file(x, stop_before_pixels=True)
    elif isinstance(x, pydicom.dataset.FileDataset):
        dicomHdr = x
    Tag = pydicom.tag.Tag(tag);
    if Tag in dicomHdr:
        tagValue = dicomHdr[Tag].value
    else:
        tagValue = None
    return tagValue

def sortMultiEcho(allFileNames,path): # ============================================
    # Sort the DICOM files from the working directory
    dicomHdr     = pydicom.read_file(allFileNames[0], stop_before_pixels=True)
    nImages      = int      (returnTagValue(dicomHdr, ('0020','1002')))
    if returnTagValue(dicomHdr, ('0020','0105')) is None :
        sys.exit("skipping")
    nRepetitions = int      (returnTagValue(dicomHdr, ('0020','0105')))
    nSlices      = int      (returnTagValue(dicomHdr, ('0021','104f')))
    EchoTime0    = float    (returnTagValue(dicomHdr, ('0018','0081')))
    echoTimeDiff = float    (returnTagValue(dicomHdr, ('0019','10ac')))
    nEchoes      = int(float(returnTagValue(dicomHdr, ('0019','10a9'))))
    nEchoesCalc  = int(nImages / nSlices)
    nImagesExp   = nImages * nRepetitions
    nImagesDir   = len(allFileNames)
    print("Number of echoes, from the header:   %s"   % nEchoes)
    print("Number of echoes, calculated:        %s"   % nEchoesCalc)
    print("Number of slices:                    %s"   % nSlices)
    print("Number of slices * echoes:           %s"   % nImages)
    print("Number of repetitions is:            %s"   % nRepetitions)
    print("Expected number of images:           %s"   % nImagesExp)
    print("Number of images in directory:       %s"   % nImagesDir)
    print("Time of first echo (ms):             %s"   % EchoTime0)
    print("Time difference between echoes (ms): %s\n" % echoTimeDiff)
    
    # Each slice an index number. The following while loop reads files until it 
    # finds nImages distinct index numbers
    sliceIndexList = list()
    fileCount = 0
    while len(sliceIndexList) < nImages:
        # sliceIndex is the counter from the first to last of nImages (across echoes and slices and volumes)
        sliceIndex = int(returnTagValue(allFileNames[fileCount], ('0019','10a2')))
        if sliceIndex not in sliceIndexList:
            sliceIndexList.append(sliceIndex)
        fileCount += 1
    sliceIndexList.sort()

    if( nImages % nSlices ):  # i.e. if there is a remainder from this division
        sys.exit("Error: There seem to be some un-accounted for slices.\n" \
                 "       Either the scan is not complete, or there\n" \
                 "       was an error with the data organization.")
    else:
        sliceIndexList = numpy.reshape(sliceIndexList, [nEchoes, nImages//nEchoes])

    # Track all SOP instance UIDs.
    if len(allFileNames) > (nImages*nRepetitions):
        print("Warning: Sometimes GE multi-echo DICOM series have replicated slices.")
        print("         These will be sorted out below.")

    imageInstanceUIDList     = list()
    multiEchoFilesSortedDict = dict()
    imageCount = 1
    for file2Process in allFileNames:
        imageInstanceUID = returnTagValue(file2Process, ('0008','0018'))
        sliceIndex       = returnTagValue(file2Process, ('0019','10a2'))
        if imageInstanceUID not in imageInstanceUIDList:
            imageInstanceUIDList.append(imageInstanceUID)
            multiEchoFilesSortedDict[imageInstanceUID] = [file2Process, sliceIndex]
        # Give some indication of progress
        if (imageCount % round(nImagesExp/100) == 0) or (imageCount == nImagesExp):
            updateProgress(imageCount/nImagesExp)
        imageCount += 1
    print("")

    if len(allFileNames) > (nImages*nRepetitions):
        print("After sorting out duplicates, number of images is: %s" % len(imageInstanceUIDList))
        print("Number of entries in dictionary is:                %s" % len(multiEchoFilesSortedDict))

    # At this time, we should have a dictionary of all of the files we need to
    # sort, and used to build multi-echo AFNI or NIFTI data sets.  Now, use this
    # dictionary(as it is already in memory) to do the final sorting of image
    # files.  At this point, we should not need to read anything from disk, but
    # should be able to move files to their correct locations / echo directories.
    print("Sorting images by echo and moving into sub-directories.")
    for EchoIdx in range(0, nEchoes):
        dirName = path + "_echo_%04d" % (EchoIdx + 1)
        os.mkdir(dirName)
    imageCount = 1
    for sopIDs in multiEchoFilesSortedDict.keys():
        sliceIndex = multiEchoFilesSortedDict[sopIDs][1]
        for EchoIdx in range(0, nEchoes):
            newEcho = EchoTime0 + (EchoIdx*echoTimeDiff)
            #print(newEcho)
            dirName = path + "_echo_%04d" % (EchoIdx + 1)
            if sliceIndex in sliceIndexList[EchoIdx]:
                #os.rename(multiEchoFilesSortedDict[sopIDs][0],
                #          os.path.join(dirName, multiEchoFilesSortedDict[sopIDs][0]))
                ds = pydicom.filereader.dcmread(multiEchoFilesSortedDict[sopIDs][0])
                ds.EchoTime = newEcho
                ds.save_as(os.path.join(dirName, multiEchoFilesSortedDict[sopIDs][0]))
                break
        # Give some indication of progress
        if (imageCount % round(nImagesExp/100) == 0) or (imageCount == nImagesExp):
            updateProgress(imageCount/nImagesExp)
        imageCount += 1
    print("")



# ======== [ Main ] ===========================================================
def main() :
    if hasattr(os, 'sync'):
        sync = os.sync
    else:
        import ctypes, platform
        if platform.uname()[0] != "Darwin":
            libc = ctypes.CDLL("libc.so.6")
        else:
            libc = ctypes.CDLL("/usr/lib/libc.dylib")
        def sync():
           libc.sync()
           
    (workDir, fileExt, isSorted) = processOptions(sys.argv[0:])
    #sys.stderr.write('Processing %s files in directory %s\n' % (fileExt, workDir))
    #print("This is the way: {}".format(os.path.abspath(workDir)))
    way = os.path.abspath(workDir)
    os.chdir(workDir)
    
    if isSorted:
        allFileNames = [f for f in os.listdir('echo_0001') if f.endswith(".%s" % fileExt)]
    else:
        allFileNames = [f for f in os.listdir('.')         if f.endswith(".%s" % fileExt)]
    
    if len(allFileNames) == 0:
        sys.exit("Error: No files with extension %s were found in %s." % (fileExt, workDir))
    allFileNames.sort()
    #print("First DICOM file is: %s" % allFileNames[0])
    
    # Test manufacturer
    if isSorted:
        dicomHdr = pydicom.read_file(os.path.join('echo_0001',allFileNames[0]), stop_before_pixels=True)
    else:
        dicomHdr = pydicom.read_file(allFileNames[0], stop_before_pixels=True)
    Manufacturer = returnTagValue(dicomHdr, ('0008','0070'))
    if Manufacturer != 'GE MEDICAL SYSTEMS':
        sys.exit("Error: Manufacturer \"%s\" not supported.\nThis program currently works only with GE files." % Manufacturer)
    
    # Get and reformat acquisition date and time
    AcqDate      = returnTagValue(dicomHdr, ('0008','0022'))
    AcqDate      = datetime.datetime.strptime(AcqDate,"%Y%m%d").strftime("%Y-%m-%d")
    AcqTime      = returnTagValue(dicomHdr, ('0008','0032'))
    AcqTime      = datetime.datetime.strptime(AcqTime,"%H%M%S").strftime("%H:%M:%S")
    AcqDateTime  = '%sT%s' % (AcqDate, AcqTime)
        
    # Check number of echoes and sort accordingly
    nEchoes = int(float(returnTagValue(dicomHdr, ('0019','10a9'))))
    if nEchoes == None or nEchoes <= 1:
        print('{} is not multiECHO'.format(os.path.split(workDir)[1]))
        # if isSorted:
        #     dirName   = 'echo_0001'
        #     niftiName = dirName
        #     MoveFile  = True
        # else:
        #     dirName   = '.';
        #     niftiName = 'nifti'
        #     MoveFile  = False
        # convertToNifti(dirName, niftiName,
        #                AcqDateTime=AcqDateTime, MoveFile=MoveFile)
       
    else:

        if not isSorted:
            sortMultiEcho(allFileNames,way)
            shutil.rmtree(way, ignore_errors=True)
        # for EchoIdx in range(0, nEchoes):
        #     dirName   = "echo_%04d" % (EchoIdx + 1)
        #     #niftiName = dirName
            
        #     # convertToNifti(dirName, niftiName,
        #     #                EchoTime=EchoTime0 + EchoIdx*echoTimeDiff,
        #     #                AcqDateTime=AcqDateTime, MoveFile=True)
        #     #convertToBrik(dirName)
        #%%
if __name__ == '__main__':
    main()