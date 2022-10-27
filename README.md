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

