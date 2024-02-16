Created by Andre Zugman. andre.zugman@nih.gov
This is a set of tool to interface with Robin and fmrif XNAT server.
It is intended for EDB use only, however it is modifiable to more generic approaches.
It facilitates searching subjects with SDAN ID instead of using MRN. 
This is still experimental. Use at your own risk.

The sample setup folder contains example of necessary setup. You'll need to setup a conda environment prior to using. I recommend you install mamba https://mamba.readthedocs.io/en/latest/
and setup a new environment.

You can now either search subjects by id or download all subjects from a certain date.

To see all options type xnat-tools -h

If you don't have a .netrc you will be prompted to use your password.

Changes in new version (0.2):

Searching by name will now only download if it can match BOTH name and lastname

Adapted nidb2bids from Anderson Winkler to work with XNAT output.

This is still experimental. Info is hardcoded in the script, new series will need to be added. 

If it is not there data will be added in unkwnon folder.

Made some improvements on location of downloaded files.

Solved problem where multi-echo data would result in single nifti.

Minor to 0.2.0.1 :

Saves updated Echo time to dicom header

NEW CHANGES v 0.3 :

ADDED a flag that allows to skip robin.
In this case use --not-robin followed by the MRN.
Provide SDANID as usual. The data will be anonymized to use the SDAN ID you provided.
You can potentially use with any characters, not necessarily sdanid 
(i.e.: This might come in handy if you want to use NDA GUID)
Do use this with care as you can potentially not remove PII fully.

I have also made minor changes where it won't automatically change for name if the download fails.
The name issue has been solved. I've kept the --search-name flag if it evers becomes needed again.
Now you can create a csv file for the data available for a single subject. 
Just provide the sdanid, the mrn (in the --not-robin flag).

NEW CHANGES v 0.4 :

Added Physio (HR and RR) from the scanner.
This is also added to the --dobids function. 
If you use the flag you should have the tsv and the required json.
The raw physio will be inside the nifti folder.
Check carefully!
Added the new MBME and Dwell to the dobids function.
The dobids folder now passes the bids validator except for the unknown folder (see below)
The dobids will now also output a dataset_description.json. 
This has an acknowledgment to this repository. 
Please play fair and keep the acknowledgment if you are publishing a dataset. ;-) 
You can now pass lists of ids and dates.
This is usefull to loop without having to put your password all the time.
NEW CHANGES v 0.4.1 :
Change some minor way on the naming of dicom files. This was causing some trouble for AFNI users.
NEW CHANGES v 0.4.4 :
Bunch of minor fixes.
Physio is now optional. It will only be downloaded if using the --physio flag.
If you do choose to use physio please be mindfull that it will download a bunch of files
without those files the physio data can not be matched to the scans. 
The downside is that those contain PII. Please be mindfull of your data.
I'm not responsible in any way if you misplace data or don't fully understand what you are doing.

Included anonymized _scans.tsv to BIDS directory. This will anonymize the data by subtracting X number of days.
The key is saved in the root directory. Be carefull not to share the key!!

########
For People that get "unkwnon" output in bids this is most likely files that are not needed
such as localizer scans or other reconstructions.
You can check what they are in the SeriesDescription files of the accompaining json.
If you run bidsvalidator these files will result in an error, but I have decided
to keep this here. You can manually remove them after you inspect and make sure
they are not needed.

NEW CHANGES v 0.5 :
Fixed a problem in the xnat2bids function for multi-session subjects.
The fmap identifier is now unique to the session.
There is now a possiblilty of running downloads and unzipping in parallel.
v 0.5.3: 
You can now pass a csv with a list of subjects, one subject per line.


