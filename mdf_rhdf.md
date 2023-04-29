Programs to convert MDF file to rhdf
Created by: Timothy Beggs

# List of programs and instructions:

1. mdf_rhdf.py


## 1. mdf_rhdf.py  
### Inputs:  
- mdf_file - file with mdf data  
### Outputs:  
- rhdf.csv - csv of converted mdf.  
### Arguments:  
- -mf --mdf_file: the mdf file path to parse defualts to "us_dhcp_datafile-DHCP2020_MDFRevisedColumnNames.txt"  
- -s3 --s3_bucket: s3 bucket where mdf is stored  
- -mdfk --mdf_key: mdf s3 key  
- -debug: turns on debug logger option  
### Use:  
- Will convert mdf to a hdf file that make_recon_input.py can turn into sf1 like tables. It attempts to keep the naming convention of the SAS version however slight changes were needed for consistancy ie: changing total_male1 to male1/ consolidating count# variable names  
- It will only roll up to one level so ideally this should be the most detailed level of sf1 tables desired to be reproduced.  
## Example Call:  
        python3 mdf_rhdf.py -mf us_dhcp_datafile-DHCP2020_MDFRevisedColumnNames.txt  
