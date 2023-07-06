# GCP_ingestion_analysis_tools


How to use this Google Cloud Platform ingestion and analysis tool libraries:

0. Add the libraries to the work path 
source ./GCP_bigquery_case_study_library.sh
source ./GCP_bigquery_statistic_library.sh
source ./GCP_common_header.sh



Description of libraries:

[0] GCP_bigquery_case_study_library.sh

Contains functions to:  organize a large number of zipfiles into a csv only folder, uploads csv files to GCP, bigquery query functions to view tables and run machine learning related commands (train test split, modeling).


[1] GCP_bigquery_statistic_library.sh

Contains functions to perform the : one sample t-statistic and z-statistic for a variety of cases, two sample z-statistic for a variety of cases (with or without respect to a category)


[2] GCP_common_header.sh

A function that will organize a folder of csv files into three folders: exact_match_header, similar_match_header, no_match_header. The exact_match_header files all have the same common header, so they can be uploaded directly to GCP in the next step.
