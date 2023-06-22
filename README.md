# automatic_GCP_ingestion


Options
0. Iteratively make common header names across all csv files: merge tot_header with unique headers, or rename all csv files with tot_header 


To improve
0. Selection to do model only (it miss classifies similarity for short key words like 'to', 'from'), or substring & model (most rigorous: it may cut selections that have the same meaning but are lexically different)
1. better threshold selection of substring & model than 70 percent similarity
2. automatic looping of similar_match folders using a decision tree structure to rename all header words 
3. for no_match folders : addition of null columns and column ordering , using the merge tot_header option 
4. restrain modifying the name to the header only - I change the name throughout the csv
5. to automate: read in header, output schema, output SQL
