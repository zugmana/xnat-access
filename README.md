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


