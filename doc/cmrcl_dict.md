| Column # | Header | Format | Description |
| --- | --- | --- | --- |
| 1 | geoid_block | %015d | Census Tabulation Block ID, formed as the concatenation of codes for State, County, Tract, and Block |
| 2 | pik | N/A[^1] | Protected Identification Key assigned to commerical record |
| 3 | sex | %1d | Binary sex |
| 4 | age | %d | Age in years |
| 5 | r | %1.17f | Uniform random variable used to sort commercial records at different stages of reid matching

[^1]: Formatting of the Protected Identification Key is considered sensitive
