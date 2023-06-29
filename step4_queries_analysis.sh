#!/bin/bash

# cd /home/oem2/Documents/ONLINE_CLASSES/Spécialisation_Google_Data_Analytics/3_Google_Data_Analytics_Capstone_Complete_a_Case_Study/git2/automatic_GCP_ingestion

# ./step4_queries_analysis.sh


clear




# ---------------------------------------------
# Setup google cloud sdk path settings
# ---------------------------------------------
export val=$(echo "X1")

if [[ $val == "X0" ]]
then 
    # Google is SDK is not setup correctly - one needs to relink to the gcloud CLI everytime you restart the PC
    source '/usr/lib/google-cloud-sdk/path.bash.inc'
    source '/usr/lib/google-cloud-sdk/completion.bash.inc'
    export PATH="/usr/lib/google-cloud-sdk/bin:$PATH"
    
    # Get latest version of the Google Cloud CLI (does not work)
    gcloud components update
else
    echo "Do not setup google cloud sdk PATH"
fi



# ---------------------------------------------
# Obtenir des informations Authorization
# ---------------------------------------------
export val=$(echo "X1")

if [[ $val == "X0" ]]
then 
    # Way 0 : gcloud init


    # Way 1 : gcloud auth login

    # A browser pop-up allows you to authorize with your Google account
    gcloud auth login

    # ******* need to set up this
    # gcloud auth login --no-launch-browser
    # gcloud auth login --cred-file=CONFIGURATION_OR_KEY_FILE
    
    # Allow for google drive access, moving files to/from GCP and google drive
    # gcloud auth login --enable-gdrive-access
    
else
    echo ""
    # echo "List active account name"
    # gcloud auth list
fi




# ---------------------------------------------
# Make random number for creating variable or names
# ---------------------------------------------
if [[ $val == "X1" ]]
then 
    let "randomIdentifier=$RANDOM*$RANDOM"
else
    let "randomIdentifier=202868496"
fi

# ---------------------------------------------



# ---------------------------------------------
# Set Desired location
# https://cloud.google.com/bigquery/docs/locations
# ---------------------------------------------
# Set the project region/location
# export location=$(echo "europe-west9-b")  # Paris
export location=$(echo "europe-west9")  # Paris
# export location=$(echo "EU")
# export location=$(echo "US")   # says US is the global option

# ---------------------------------------------





# ---------------------------------------------
# ENABLE API Services
# ---------------------------------------------
export val=$(echo "X0")

if [[ $val == "X0" ]]
then

    gcloud services enable iam.googleapis.com \
        bigquery.googleapis.com \
        logging.googleapis.com
  
fi

# ---------------------------------------------






# ---------------------------------------------
# SELECT PROJECT_ID
# ---------------------------------------------
export val=$(echo "X0")

if [[ $val == "X0" ]]
then 
    # List projects
    # gcloud config list project
    
    # Set project
    export PROJECT_ID=$(echo "northern-eon-377721")
    gcloud config set project $PROJECT_ID

    # List DATASETS in the current project
    # bq ls $PROJECT_ID:
    # OR
    # bq ls

    #  datasetId         
    #  ------------------------ 
    #   babynames               
    #   city_data               
    #   google_analytics_cours  
    #   test       

    # ------------------------

fi

# ---------------------------------------------







# ---------------------------------------------
# SELECT dataset_name
# ---------------------------------------------
export val=$(echo "X0")

if [[ $val == "X0" ]]
then 

    # Create a new DATASET named PROJECT_ID
    # export dataset_name=$(echo "google_analytics")
    # bq --location=$location mk $PROJECT_ID:$dataset_name

    # OR 

    # Use existing dataset
    export dataset_name=$(echo "google_analytics")

    # ------------------------

    # List TABLES in the dataset
    # echo "bq ls $PROJECT_ID:$dataset_name"
    # bq --location=$location ls $PROJECT_ID:$dataset_name

    #           tableId            Type    Labels   Time Partitioning   Clustered Fields  
    #  -------------------------- ------- -------- ------------------- ------------------ 
    #   avocado_data               TABLE                                                  
    #   departments                TABLE                                                  
    #   employees                  TABLE                                                  
    #   orders                     TABLE                                                  
    #   student-performance-data   TABLE                                                  
    #   warehouse                  TABLE

    # ------------------------

    # echo "bq show $PROJECT_ID:$dataset_name"
    # bq --location=$location show $PROJECT_ID:$dataset_name

    #    Last modified             ACLs             Labels    Type     Max time travel (Hours)  
    #  ----------------- ------------------------- -------- --------- ------------------------- 
    #   08 Mar 11:40:52   Owners:                            DEFAULT   168                      
    #                       j622amilah@gmail.com,                                               
    #                       projectOwners                                                       
    #                     Writers:                                                              
    #                       projectWriters                                                      
    #                     Readers:                                                              
    #                       projectReaders  

    # ------------------------

fi


# ---------------------------------------------







# 0. Determine if data is joinable to try to connect the two tables
export val=$(echo "X1")

if [[ $val == "X0" ]]
then 
     
     # TABLE0: ride_id, rideable_type, started_at, ended_at, start_station_name, start_station_id, end_station_name, end_station_id, start_lat, start_lng, end_lat, end_lng, member_casual
     export TABLE_name0=$(echo "bikeshare_table0")
     
     # TABLE1: trip_id, starttime, stoptime, bikeid, tripduration, start_station_id, start_station_name, end_station_id, end_station_name, usertype, gender, birthyear
     export TABLE_name1=$(echo "bikeshare_table1")
     
     export TABLE_name_join=$(echo "bikeshare_full")
     
     
     bq query \
            --location=$location \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT * FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name0'`;'

fi


# ---------------------------------------------


# 0. Determine if data is joinable to try to connect the two tables
export val=$(echo "X1")

if [[ $val == "X0" ]]
then 
     
     # ride_id, rideable_type, started_at, ended_at, start_station_name, start_station_id, end_station_name, end_station_id, start_lat, start_lng, end_lat, end_lng, member_casual
     export TABLE_name0=$(echo "bikeshare_table0")
     
     # trip_id, starttime, stoptime, bikeid, tripduration, start_station_id, start_station_name, end_station_id, end_station_name, usertype, gender, birthyear
     export TABLE_name1=$(echo "bikeshare_table1")
     
     export TABLE_name_join=$(echo "bikeshare_full")  # ride_id_join

     bq query \
            --location=$location \
            --destination_table $PROJECT_ID:$dataset_name.$TABLE_name_join \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT T0.ride_id, 
            T0.rideable_type, 
            T0.started_at, 
            T0.ended_at, 
            T0.start_station_name AS stsname_T0,
            T0.start_station_id AS ssid_T0,
            T0.end_station_name AS esname_T0,
            T0.end_station_id AS esid_T0,
            T0.start_lat, 
            T0.start_lng, 
            T0.end_lat, 
            T0.end_lng, 
            T0.member_casual,
            T1.trip_id, 
            T1.starttime, 
            T1.stoptime, 
            T1.bikeid, 
            T1.tripduration, 
            T1.start_station_id AS ssid_T1, 
            T1.start_station_name AS stsname_T1, 
            T1.end_station_id AS esid_T1, 
            T1.end_station_name AS esname_T1, 
            T1.usertype, 
            T1.gender, 
            T1.birthyear FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name0'` AS T0
FULL JOIN `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name1'` AS T1 ON T0.ride_id = T1.trip_id;'   

# Try 0: More unique identifier => problem: usertype, gender, birthyear are NULL for member_casual, rideable_type, which means that the Tables just do not align. Even if I stacked the tables and made a common header they would STILL not ALIGN. problem solved, there is no corresponding data.
# FULL JOIN `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name1'` AS T1 ON T0.ride_id = T1.trip_id
# JOIN ON ride_id=trip_id DOES NOTHING because TABLE0 (member_casual, rideable_type) does not align with TABLE1 (usertype, gender, birthyear)

# Try 1: Less unique identifier (does not work)
# FULL JOIN `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name1'` AS T1 ON T0.start_station_id = T1.start_station_id
# FULL JOIN `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name1'` AS T1 ON T0.end_station_id = T1.end_station_id
# FULL JOIN `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name1'` AS T1 ON T0.ride_id = T1.bikeid

fi


# ---------------------------------------------


export val=$(echo "X1")

if [[ $val == "X0" ]]
then 
	# List all tables
	export location=$(echo "europe-west9")
	export PROJECT_ID=$(echo "northern-eon-377721")
	export dataset_name=$(echo "google_analytics")
	bq --location=$location ls $PROJECT_ID:$dataset_name

fi

# Great! worked!


# ---------------------------------------------


export val=$(echo "X1")

if [[ $val == "X0" ]]
then 
	# export TABLE_name=$(echo "bikeshare_full")
	# export TABLE_name=$(echo "bikeshare_full_clean0")
	export TABLE_name=$(echo "bikeshare_full_clean1")
    
    # List only the column names
    bq query \
    --location=$location \
    --allow_large_results \
    --use_legacy_sql=false \
    "SELECT column_name, data_type
FROM $PROJECT_ID.$dataset_name.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME='$TABLE_name';"
    
    # +---------------+
    # |  column_name  |
    # +---------------+
    # | ride_id       |
    # | rideable_type |
    # | started_at    |
    # | ended_at      |
    # | stsname_T0    |
    # | ssid_T0       |
    # | esname_T0     |
    # | esid_T0       |
    # | start_lat     |
    # | start_lng     |
    # | end_lat       |
    # | end_lng       |
    # | member_casual |
    # | trip_id       |
    # | starttime     |
    # | stoptime      |
    # | bikeid        |
    # | tripduration  |
    # | ssid_T1       |
    # | stsname_T1    |
    # | esid_T1       |
    # | esname_T1     |
    # | usertype      |
    # | gender        |
    # | birthyear     |
    # +---------------+

    
    # Lists many specifications of a table: 
    # table_catalog    |   table_schema   |   table_name   |  column_name  | ordinal_position | is_nullable | data_type | is_generated | generation_expression | is_stored | is_hidden | is_updatable | is_system_defined | is_partitioning_column | clustering_ordinal_position | collation_name | column_default | rounding_mode
    # "SELECT *
    # FROM $PROJECT_ID.$dataset_name.INFORMATION_SCHEMA.COLUMNS
    # WHERE TABLE_NAME='$TABLE_name';"
    
    # OR 
    
    # SELECT *
    # FROM $PROJECT_ID.$dataset_name.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
    # WHERE TABLE_NAME='$TABLE_name';"
fi

# ---------------------------------------------




# ---------------------------------------------


# 1. Identify the main features for the analysis
export val=$(echo "X1")

if [[ $val == "X0" ]]
then 
    
    export TABLE_name=$(echo "bikeshare_full")
    
    export TABLE_name_clean=$(echo "bikeshare_full_clean0")
     
     bq query \
            --location=$location \
            --destination_table $PROJECT_ID:$dataset_name.$TABLE_name_clean \
            --allow_large_results \
            --use_legacy_sql=false \
    'SELECT IF(ride_id = trip_id AND ride_id IS NOT NULL, NULL, COALESCE(ride_id, trip_id)) AS fin_trip_ID,
    rideable_type,
    IF(started_at = starttime AND started_at IS NOT NULL, NULL, COALESCE(started_at, starttime)) AS fin_starttime,
    IF(ended_at = stoptime AND ended_at IS NOT NULL, NULL, COALESCE(ended_at, stoptime)) AS fin_endtime,
    IF(stsname_T0 = stsname_T1 AND stsname_T0 IS NOT NULL, NULL, COALESCE(stsname_T0, stsname_T1)) AS fin_stsname,
    IF(ssid_T0 = ssid_T1 AND ssid_T0 IS NOT NULL, NULL, COALESCE(ssid_T0, ssid_T1)) AS fin_stsID,
    IF(esname_T0 = esname_T1 AND esname_T0 IS NOT NULL, NULL, COALESCE(esname_T0, esname_T1)) AS fin_esname,
    IF(esid_T0 = esid_T1 AND esid_T0 IS NOT NULL, NULL, COALESCE(esid_T0, esid_T1)) AS fin_esID,
    SAFE_CAST(start_lat AS FLOAT64) AS start_lat_NUM,
    SAFE_CAST(start_lng AS FLOAT64) AS start_lng_NUM,
    SAFE_CAST(end_lat AS FLOAT64) AS end_lat_NUM,
    SAFE_CAST(end_lng AS FLOAT64) AS end_lng_NUM,
    member_casual,
    bikeid,
    tripduration,
    usertype,
    gender,
    birthyear
     FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`;'
    

fi


# ---------------------------------------------

# Reduce the table with 3 items

export val=$(echo "X1")

if [[ $val == "X0" ]]
then 
    
    export TABLE_name=$(echo "bikeshare_full")
    
    export TABLE_name_clean_prev=$(echo "bikeshare_full_clean0") 
    
    export TABLE_name_clean=$(echo "bikeshare_full_clean1") 
    
     bq query \
            --location=$location \
            --destination_table $PROJECT_ID:$dataset_name.$TABLE_name_clean \
            --allow_large_results \
            --use_legacy_sql=false \
    'WITH temptable AS ( 
    SELECT fin_trip_ID,
    rideable_type,
    CAST(fin_starttime AS TIMESTAMP) AS start_TIMESTAMP,
    CAST(fin_endtime AS TIMESTAMP) AS end_TIMESTAMP,
    fin_stsname,
    fin_stsID,
    fin_esname,
    fin_esID,
    IF(start_lat_NUM > end_lat_NUM, start_lat_NUM, end_lat_NUM) AS MAX_LAT,
    IF(start_lat_NUM < end_lat_NUM, start_lat_NUM, end_lat_NUM) AS MIN_LAT,
    IF(start_lng_NUM > end_lng_NUM, start_lng_NUM, end_lng_NUM) AS MAX_LONG,
    IF(start_lng_NUM < end_lng_NUM, start_lng_NUM, end_lng_NUM) AS MIN_LONG,
    member_casual AS member_casual_T0,
    bikeid,
    tripduration,
    (CASE WHEN usertype="Customer" then "casual" WHEN usertype="Subscriber" then "member" WHEN usertype="Dependent" then "casual" end) AS member_casual_T1,
    gender,
    birthyear
     FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name_clean_prev'`)
     SELECT fin_trip_ID,
     rideable_type,
     IF(CAST(tripduration AS INTEGER) > TIMESTAMP_DIFF(end_TIMESTAMP, start_TIMESTAMP, SECOND), CAST(tripduration AS INTEGER), TIMESTAMP_DIFF(end_TIMESTAMP, start_TIMESTAMP, SECOND)) AS trip_time,
     fin_stsname,
    fin_stsID,
    fin_esname,
    fin_esID,
    SAFE_CAST(ABS(POWER(MAX_LAT-MIN_LAT, 2) + POWER(MAX_LONG-MIN_LONG, 2)) AS FLOAT64) AS trip_distance,
    COALESCE(member_casual_T0, member_casual_T1) AS member_casual,
    CAST(bikeid AS INTEGER) AS bikeid_INT,
    gender,
    CAST(birthyear AS INTEGER) AS birthyear_INT
     FROM temptable;'
    
fi





# ---------------------------------------------
# Statistics 
# https://cloud.google.com/bigquery/docs/reference/standard-sql/statistical_aggregate_functions#stddev
# ---------------------------------------------
# 2. Question 0: How do annual members and casual riders use Cyclistic bikes diﬀerently?

# Can only evaluate TABLE0 (member_casual, rideable_type) does not align with TABLE1 (usertype, gender, birthyear)

# ************************************
# Evaluation of numerical features ONLY
# ************************************
export val=$(echo "X1")

if [[ $val == "X0" ]]
then 
	export TABLE_name=$(echo "bikeshare_full_clean1")
	
	
	
	# ******************
	# SQL z/t-statistic library and p-value calculation
	# ******************
	
	# [ONE SAMPLE TESTS : testing significance with respect to the mean of a numerical feature FOR a categorical feature category (sample population) ]
	# Determine if a numerical feature for a category of another feature is statistically different than the mean of the numerical feature regardless of considering the categorical feature
	
	# Does being a member mean that trip_time is significantly lower with respect to the mean of trip_time (regardless of being a member or not)?
	# ********* CHANGE *********
	# Numerical feature:
	# export samp1_FEAT_name=$(echo "trip_distance")
	# export samp1_FEAT_name=$(echo "trip_time")
	export samp1_FEAT_name=$(echo "birthyear_INT")
	# Categorical feature:
	export category_FEAT_name=$(echo "member_casual")
	# ********* CHANGE *********
	# [0] z_statistic_ONE_SAMPLE : Comparing the sample population mean with the population mean (large sample size populations)
	# [1] t_statistic_ONE_SAMPLE : Comparing the sample population mean with the population mean (small sample size populations)
	bq query \
            --location=$location \
            --allow_large_results \
            --use_legacy_sql=false \
    'CREATE TEMP FUNCTION z_statistic_ONE_SAMPLE(samp1_mean FLOAT64, pop_mean FLOAT64, pop_std FLOAT64, samp1_len INT64)
AS (
  (samp1_mean - pop_mean)/(pop_std/SQRT(samp1_len))
  ); 
  
  CREATE TEMP FUNCTION t_statistic_ONE_SAMPLE(samp1_mean FLOAT64, pop_mean FLOAT64, samp1_std FLOAT64, samp1_len INT64)
AS (
  (samp1_mean - pop_mean)/(samp1_std/SQRT(samp1_len))
    );
    
    WITH shorttab2 AS
(
  SELECT
  '$category_FEAT_name',
  AVG('$samp1_FEAT_name') AS avg_samp1_VAR,
  AVG(CAST('$samp1_FEAT_name' AS FLOAT64)) AS samp1_mean,
  STDDEV(CAST('$samp1_FEAT_name' AS FLOAT64)) AS samp1_std,
  (SELECT AVG(CAST('$samp1_FEAT_name' AS FLOAT64)) FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`) AS pop_mean,
  (SELECT STDDEV(CAST('$samp1_FEAT_name' AS FLOAT64)) FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`) AS pop_std,
  CAST(COUNT(*) AS INT64) AS samp1_len 
  FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`
  GROUP BY '$category_FEAT_name'
  )
  SELECT '$category_FEAT_name',
  z_statistic_ONE_SAMPLE(samp1_mean, pop_mean, pop_std, samp1_len) AS z_critical_onesample,
  t_statistic_ONE_SAMPLE(samp1_mean, pop_mean, samp1_std, samp1_len) AS t_critical_onesample,
  avg_samp1_VAR,
  samp1_len AS df_sample_number
  FROM shorttab2;'
	
	
	# Numerical features:
	# export samp1_FEAT_name=$(echo "trip_distance")
	# Categorical features:
	# export category_FEAT_name=$(echo "member_casual")
	# +---------------+----------------------+----------------------+-----------------------+------------------+
	# | member_casual | z_critical_onesample | t_critical_onesample |     avg_samp1_VAR     | df_sample_number |
	# +---------------+----------------------+----------------------+-----------------------+------------------+
	# | casual        |  -0.8883783713928888 |   -1.136152360853619 | 0.0037317409782921572 |         11647954 |
	# | member        |   0.8792376529599473 |   0.7805120094229401 |  0.006629366046007331 |         23815154 |
	# +---------------+----------------------+----------------------+-----------------------+------------------+

	# https://www.socscistatistics.com/pvalues/normaldistribution.aspx
	# Z score: -0.025, P-Value is .490027 , not significant
	# Z score: 0.0521, P-Value is .479225 , not significant
	
	# But, there is a twice as much data for members than casual users, which makes the statistic more significant (biased)
	# There is a trend showing that members have a lower avg_trip_time than casual members. Members use the bikes and return them directly, while casual members keep the bikes longer.
	
	# ---------------------------------------------
	
	# Numerical features:
	# export samp1_FEAT_name=$(echo "trip_time")
	# Categorical features:
	# export category_FEAT_name=$(echo "member_casual")
	# +---------------+----------------------+----------------------+-------------------+------------------+
	# | member_casual | z_critical_onesample | t_critical_onesample |   avg_samp1_VAR   | df_sample_number |
	# +---------------+----------------------+----------------------+-------------------+------------------+
	# | member        |  -124.50965197887787 |  -206.05286801679352 | 769.1156600540992 |         23815154 |
	# | casual        |   178.03479666544257 |    117.5464128204206 | 2118.084874734224 |         11647954 |
	# +---------------+----------------------+----------------------+-------------------+------------------+


	# ---------------------------------------------
	
	# Numerical features:
	# export samp1_FEAT_name=$(echo "birthyear_INT")
	# Categorical features:
	# export category_FEAT_name=$(echo "member_casual")
	# +---------------+----------------------+----------------------+--------------------+------------------+
	# | member_casual | z_critical_onesample | t_critical_onesample |   avg_samp1_VAR    | df_sample_number |
	# +---------------+----------------------+----------------------+--------------------+------------------+
	# | member        |   -75.84176670592645 |   -75.96042235114496 |  1981.194184606505 |         23815154 |
	# | casual        |    1892.533427644269 |   2122.1930117305938 | 1987.4188076871403 |         11647954 |
	# +---------------+----------------------+----------------------+--------------------+------------------+


fi


# ---------------------------------------------


# ************************************
# Evaluation of categorical features ONLY  # to test next!
# ************************************
export val=$(echo "X1")

if [[ $val == "X0" ]]
then 
	export TABLE_name=$(echo "bikeshare_full_clean1")
	
	
	
	# ******************
	# SQL z/t-statistic library and p-value calculation
	# ******************
	
	# [ONE SAMPLE TESTS : testing significance with respect to the mean of a numerical feature FOR a categorical feature category (sample population) ]
	# Determine if a numerical feature for a category of another feature is statistically different than the mean of the numerical feature regardless of considering the categorical feature
	
	# Does being a member mean that trip_time is significantly lower with respect to the mean of trip_time (regardless of being a member or not)?
	# ********* CHANGE *********
	# Transform categorical feature into a Numerical feature:
	export samp1_FEAT_name=$(echo "rideable_type")
	# export samp1_FEAT_name=$(echo "gender")
	
	# Categorical feature:
	export category_FEAT_name=$(echo "member_casual")
	# ********* CHANGE *********
	# [0] z_statistic_ONE_SAMPLE : Comparing the sample population mean with the population mean (large sample size populations)
	# [1] t_statistic_ONE_SAMPLE : Comparing the sample population mean with the population mean (small sample size populations)
	bq query \
            --location=$location \
            --allow_large_results \
            --use_legacy_sql=false \
    'CREATE TEMP FUNCTION z_statistic_ONE_SAMPLE(samp1_mean FLOAT64, pop_mean FLOAT64, pop_std FLOAT64, samp1_len INT64)
AS (
  (samp1_mean - pop_mean)/(pop_std/SQRT(samp1_len))
  ); 
  
  CREATE TEMP FUNCTION t_statistic_ONE_SAMPLE(samp1_mean FLOAT64, pop_mean FLOAT64, samp1_std FLOAT64, samp1_len INT64)
AS (
  (samp1_mean - pop_mean)/(samp1_std/SQRT(samp1_len))
    );
    
    WITH shorttab2 AS
(
  SELECT 
  ML.LABEL_ENCODER('$samp1_FEAT_name') OVER () AS transformed_FEAT,
  '$category_FEAT_name',
  AVG(transformed_FEAT) AS avg_samp1_VAR,
  AVG(CAST(transformed_FEAT AS FLOAT64)) AS samp1_mean,
  STDDEV(CAST(transformed_FEAT AS FLOAT64)) AS samp1_std,
  (SELECT AVG(CAST(transformed_FEAT AS FLOAT64)) FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`) AS pop_mean,
  (SELECT STDDEV(CAST(transformed_FEAT AS FLOAT64)) FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`) AS pop_std,
  CAST(COUNT(*) AS INT64) AS samp1_len 
  FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`
  GROUP BY '$category_FEAT_name'
  )
  SELECT '$category_FEAT_name',
  z_statistic_ONE_SAMPLE(samp1_mean, pop_mean, pop_std, samp1_len) AS z_critical_onesample,
  t_statistic_ONE_SAMPLE(samp1_mean, pop_mean, samp1_std, samp1_len) AS t_critical_onesample,
  avg_samp1_VAR,
  samp1_len AS df_sample_number
  FROM shorttab2;'
	
	
	# Numerical features:
	# export samp1_FEAT_name=$(echo "trip_distance")
	# Categorical features:
	# export category_FEAT_name=$(echo "member_casual")
	# +---------------+----------------------+----------------------+-----------------------+------------------+
	# | member_casual | z_critical_onesample | t_critical_onesample |     avg_samp1_VAR     | df_sample_number |
	# +---------------+----------------------+----------------------+-----------------------+------------------+
	# | casual        |  -0.8883783713928888 |   -1.136152360853619 | 0.0037317409782921572 |         11647954 |
	# | member        |   0.8792376529599473 |   0.7805120094229401 |  0.006629366046007331 |         23815154 |
	# +---------------+----------------------+----------------------+-----------------------+------------------+

	# https://www.socscistatistics.com/pvalues/normaldistribution.aspx
	# Z score: -0.025, P-Value is .490027 , not significant
	# Z score: 0.0521, P-Value is .479225 , not significant
	
	# But, there is a twice as much data for members than casual users, which makes the statistic more significant (biased)
	# There is a trend showing that members have a lower avg_trip_time than casual members. Members use the bikes and return them directly, while casual members keep the bikes longer.
	
	# ---------------------------------------------
	
	# Numerical features:
	# export samp1_FEAT_name=$(echo "trip_time")
	# Categorical features:
	# export category_FEAT_name=$(echo "member_casual")
	# +---------------+----------------------+----------------------+-------------------+------------------+
	# | member_casual | z_critical_onesample | t_critical_onesample |   avg_samp1_VAR   | df_sample_number |
	# +---------------+----------------------+----------------------+-------------------+------------------+
	# | member        |  -124.50965197887787 |  -206.05286801679352 | 769.1156600540992 |         23815154 |
	# | casual        |   178.03479666544257 |    117.5464128204206 | 2118.084874734224 |         11647954 |
	# +---------------+----------------------+----------------------+-------------------+------------------+


	# ---------------------------------------------
	
	# Numerical features:
	# export samp1_FEAT_name=$(echo "birthyear_INT")
	# Categorical features:
	# export category_FEAT_name=$(echo "member_casual")
	# +---------------+----------------------+----------------------+--------------------+------------------+
	# | member_casual | z_critical_onesample | t_critical_onesample |   avg_samp1_VAR    | df_sample_number |
	# +---------------+----------------------+----------------------+--------------------+------------------+
	# | member        |   -75.84176670592645 |   -75.96042235114496 |  1981.194184606505 |         23815154 |
	# | casual        |    1892.533427644269 |   2122.1930117305938 | 1987.4188076871403 |         11647954 |
	# +---------------+----------------------+----------------------+--------------------+------------------+


fi


# ---------------------------------------------


export val=$(echo "X0")

if [[ $val == "X0" ]]
then 
	export TABLE_name=$(echo "bikeshare_full_clean1")
	
	
	# ********* CHANGE *********
	# Numerical feature:
	# export samp1_FEAT_name=$(echo "trip_distance")
	export samp1_FEAT_name=$(echo "trip_time")
	# export samp1_FEAT_name=$(echo "birthyear_INT")
	
	# Categorical feature:
	export category_FEAT_name=$(echo "member_casual")
	export category_FEAT_name_VAR1=$(echo "'member'")
	export category_FEAT_name_VAR2=$(echo "'casual'")
	
	export TABLETEMP_twosample_table=$(echo "twosample_table") 
	# ********* CHANGE *********
	# [2] t_OR_Z_statistic_TWO_SAMPLE : Comparing the the means of two sample populations
	bq query \
            --location=$location \
            --allow_large_results \
            --use_legacy_sql=false \
            
            
            
     'WITH shorttab1 AS
(


$       
            
     # Need to save 
    'WITH shorttab1 AS
(
SELECT CAST('$samp1_FEAT_name' AS FLOAT64) AS x, ROW_NUMBER() OVER(ORDER BY fin_trip_ID) AS num_row 
 WHERE '$category_FEAT_name'='$category_FEAT_name_VAR1'
    FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`
    )
    SELECT
    SUM(POW(x - (samp1_mean+(num_row*0)), 2)) AS samp1_sum,
    FROM shorttab1;
    '
            
            
     # Scalars      
    'WITH shorttab2 AS
(
  SELECT
  (SELECT AVG(CAST('$samp1_FEAT_name' AS FLOAT64)) FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'` WHERE '$category_FEAT_name'='$category_FEAT_name_VAR1') AS samp1_mean,
  (SELECT AVG(CAST('$samp1_FEAT_name' AS FLOAT64)) FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'` WHERE '$category_FEAT_name'='$category_FEAT_name_VAR2') AS samp2_mean,
  (SELECT STDDEV(CAST('$samp1_FEAT_name' AS FLOAT64)) FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'` WHERE '$category_FEAT_name'='$category_FEAT_name_VAR1') AS samp1_std,
  (SELECT STDDEV(CAST('$samp1_FEAT_name' AS FLOAT64)) FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'` WHERE '$category_FEAT_name'='$category_FEAT_name_VAR2') AS samp2_std,
  (SELECT CAST(COUNT(*) AS INT64) FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'` WHERE '$category_FEAT_name'='$category_FEAT_name_VAR1') AS samp1_len,
  (SELECT CAST(COUNT(*) AS INT64) FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'` WHERE '$category_FEAT_name'='$category_FEAT_name_VAR2') AS samp2_len
  FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`
  LIMIT 1
  )
  SELECT
  samp1_mean,
  samp2_mean,
  samp1_std,
  samp2_std,
  samp1_len,
  samp2_len,
  
  FROM shorttab2
  LIMIT 1;'


fi


export val=$(echo "X1")

if [[ $val == "X0" ]]
then 


# ---------------------------------------------


export val=$(echo "X1")

if [[ $val == "X0" ]]
then 
	export TABLE_name=$(echo "bikeshare_full_clean1")
	
	

	# [TWO SAMPLE TESTS : testing significance between two categorical features by comparing the means of another numerical feature]
	# Determine if two categorical features have statistically different means for another numerical feature
	
	# Does being a member signify that trip_time is significantly different than being a casual user?
	
	# ********* CHANGE *********
	# Numerical feature:
	# export samp1_FEAT_name=$(echo "trip_distance")
	export samp1_FEAT_name=$(echo "trip_time")
	# export samp1_FEAT_name=$(echo "birthyear_INT")
	
	# Categorical feature:
	export category_FEAT_name=$(echo "member_casual")
	export category_FEAT_name_VAR1=$(echo "'member'")
	export category_FEAT_name_VAR2=$(echo "'casual'")
	
	
	export TABLETEMP_twosample_table=$(echo "twosample_table") 
	# ********* CHANGE *********
	# [2] t_OR_Z_statistic_TWO_SAMPLE : Comparing the the means of two sample populations
	bq query \
            --location=$location \
            --destination_table $PROJECT_ID:$dataset_name.$TABLETEMP_twosample_table \
            --allow_large_results \
            --use_legacy_sql=false \
    'WITH shorttab2 AS
(
  SELECT
  (SELECT AVG(CAST('$samp1_FEAT_name' AS FLOAT64)) FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'` WHERE '$category_FEAT_name'='$category_FEAT_name_VAR1') AS samp1_mean,
  (SELECT AVG(CAST('$samp1_FEAT_name' AS FLOAT64)) FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'` WHERE '$category_FEAT_name'='$category_FEAT_name_VAR2') AS samp2_mean,
  (SELECT STDDEV(CAST('$samp1_FEAT_name' AS FLOAT64)) FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'` WHERE '$category_FEAT_name'='$category_FEAT_name_VAR1') AS samp1_std,
  (SELECT STDDEV(CAST('$samp1_FEAT_name' AS FLOAT64)) FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'` WHERE '$category_FEAT_name'='$category_FEAT_name_VAR2') AS samp2_std,
  (SELECT CAST(COUNT(*) AS INT64) FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'` WHERE '$category_FEAT_name'='$category_FEAT_name_VAR1')  AS samp1_len,
  (SELECT CAST(COUNT(*) AS INT64) FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'` WHERE '$category_FEAT_name'='$category_FEAT_name_VAR2')  AS samp2_len
  FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`
  )
  SELECT
  samp1_mean,
  samp2_mean,
  samp1_std,
  samp2_std,
  samp1_len,
  samp2_len,
  SUM(POW((SELECT CAST('$samp1_FEAT_name' AS FLOAT64) FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'` WHERE '$category_FEAT_name'='$category_FEAT_name_VAR1') - samp1_mean, 2)) AS samp1_sum,
  SUM(POW((SELECT CAST('$samp1_FEAT_name' AS FLOAT64) FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'` WHERE '$category_FEAT_name'='$category_FEAT_name_VAR2') - samp2_mean, 2)) AS samp2_sum
  FROM shorttab2;'
  
  bq query \
            --location=$location \
            --allow_large_results \
            --use_legacy_sql=false \
    'CREATE TEMP FUNCTION z_statistic_TWO_SAMPLE(samp1_mean FLOAT64, samp2_mean FLOAT64, samp1_std FLOAT64, samp2_std FLOAT64, samp1_len INT64, samp2_len INT64)
AS (
    (samp1_mean - samp2_mean)/SQRT( ((samp1_std*samp1_std)/samp1_len) + ((samp2_std*samp2_std)/samp2_len))
    );
    
    CREATE TEMP FUNCTION t_statistic_TWO_SAMPLE(samp1_sum FLOAT64, samp2_sum FLOAT64, samp1_mean FLOAT64, samp2_mean FLOAT64, samp1_std FLOAT64, samp2_std FLOAT64, samp1_len INT64, samp2_len INT64)
AS (
    (samp1_mean - samp2_mean)/ ( SQRT( (samp1_sum + samp2_sum ) / (samp1_len + samp2_len - 2) ) * SQRT( 1/samp1_len + 1/samp2_len ) )
    );
    
    
  SELECT 
  z_statistic_TWO_SAMPLE(samp1_mean, samp2_mean, samp1_std, samp2_std, samp1_len, samp2_len) AS z_critical_twosample,
  t_statistic_TWO_SAMPLE(samp1_sum, samp2_sum, samp1_mean, samp2_mean, samp1_std, samp2_std, samp1_len, samp2_len) AS t_critical_twosample FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLETEMP_twosample_table'`'
	


fi



# ---------------------------------------------



# 3. Question 1: Why would casual riders buy Cyclistic annual memberships?




	# ---------------------------------------------
	
	# Numerical features:
	# export samp1_FEAT_name=$(echo "birthyear_INT")
	# Categorical features:
	# export category_FEAT_name=$(echo "member_casual")
	# +---------------+----------------------+----------------------+--------------------+------------------+
	# | member_casual | z_critical_onesample | t_critical_onesample |   avg_samp1_VAR    | df_sample_number |
	# +---------------+----------------------+----------------------+--------------------+------------------+
	# | member        |   -75.84176670592645 |   -75.96042235114496 |  1981.194184606505 |         23815154 |
	# | casual        |    1892.533427644269 |   2122.1930117305938 | 1987.4188076871403 |         11647954 |
	# +---------------+----------------------+----------------------+--------------------+------------------+
	
# From the onesample ttest, we learned that members closer to the mean of all bike users are members. If people become older/closer to the mean age of over all bike users, they are statistically likely to be members.

# people closer to the bike user mean age are likely to be members, and members are likely to be 6 years older than casual bike users

# 

export val=$(echo "X1")

if [[ $val == "X0" ]]
then 

	export TABLE_name=$(echo "bikeshare_full_clean1")

	bq query \
            --location=$location \
            --allow_large_results \
            --use_legacy_sql=false \
    'SELECT AVG(birthyear_INT) FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`'

fi

	# +--------------------+
	# |        f0_         |
	# +--------------------+
	# | 1981.3638807282207 |
	# +--------------------+


# ---------------------------------------------

# 4. Question 2: How can Cyclistic use digital media to inﬂuence casual riders to become members?



export val=$(echo "X1")

if [[ $val == "X0" ]]
then 
	echo ""

fi

# ---------------------------------------------





# ---------------------------------------------





# ---------------------------------------------





# ---------------------------------------------





# ---------------------------------------------





# ---------------------------------------------



# ---------------------------------------------


export val=$(echo "X1")

if [[ $val == "X0" ]]
then 
    
    echo "---------------- Query Delete Tables ----------------"
    
    export TABLE_name_join=$(echo "bikeshare_full")
    
    # bq rm -t $PROJECT_ID:$dataset_name.$TABLE_name_join
    bq rm -t $PROJECT_ID:$dataset_name.bikeshare_full_clean0
    # bq rm -t $PROJECT_ID:$dataset_name.bikeshare_full_clean1
    
fi


# ---------------------------------------------

