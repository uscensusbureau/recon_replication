| Column # | Header | Format | Description |
| --- | --- | --- | --- |
| 1 | geoid_block | %015d | Census Tabulation Block ID, formed as the concatenation of codes for State, County, Tract, and Block |
| 2 | pik | N/A[^1] | 2010 CUF Protected Identification Key |
| 3 | sex | %1d | Binary sex |
| 4 | age | %d | Age in years |
| 5 | white | %1d | Binary equal to 1 if race includes White |
| 6 | black | %1d | Binary equal to 1 if race includes Black |
| 7 | aian | %1d | Binary equal to 1 if race includes American Indian or Alaskan Native |
| 8 | asian | %1d | Binary equal to 1 if race includes Asian |
| 9 | nhopi | %1d | Binary equal to 1 if race includes Native Hawaiian or Pacific Islander |
| 10 | sor | %1d | Binary equal to 1 if race includes Some Other Race |
| 11 | hisp | %1d | Binary equal to 1 if ethnicity was given as Hispanic or Latino
| 12 | keep | %1d | Binary equal to 1 if a geoid_block-pik combination is unique within a geoid_block or, if there are duplicate records for a given geoid_block-pik, is equal to 1 if the record was selected for that geoid_block-pik. 'keep' is equal to 0 if either the pik is missing or if it was a duplicate record (by geoid_block-pik) that was not selected.
| 13 | r_keep | %1.17f | Uniform random variable that was used to select a single record in the case of duplicate geoid_block-pik combinations
| 14 | r | %1.17f | Uniform random variable used to sort cef records at different stages of reid matching

[^1]: Formatting of the Protected Identification Key is considered sensitive
