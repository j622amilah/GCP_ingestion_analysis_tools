#!/bin/bash

# cd /home/oem2/Documents/ONLINE_CLASSES/Spécialisation_Google_Data_Analytics/3_Google_Data_Analytics_Capstone_Complete_a_Case_Study/git2/automatic_GCP_ingestion

# ./step4_queries_analysis.sh



clear



# ---------------------------
# Functions START
# ---------------------------


# ---------------------------
# REUSEABLE Queries
# ---------------------------
# T-test: https://www.statology.org/t-score-p-value-calculator/
# Z-score : https://www.socscistatistics.com/pvalues/normaldistribution.aspx
ONE_SAMPLE_TESTS_t_and_zstatistic_of_NUMfeat_perCategory(){

    # [0] z_statistic_ONE_SAMPLE : Comparing the sample population mean with the population mean (large sample size populations)
    # [1] t_statistic_ONE_SAMPLE : Comparing the sample population mean with the population mean (small sample size populations)
	
    # Inputs:
    # $1 = location
    # $2 = samp1_FEAT_name (NUMERICAL feature per category to test statistical significance with the mean)
    # $3 = PROJECT_ID
    # $4 = dataset_name
    # $5 = TABLE_name
    # $6 = category_FEAT_name (CATEGORICAL feature)
	
	bq query \
            --location=$1 \
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
  '$6',
  AVG('$2') AS avg_samp1_VAR,
  AVG(CAST('$2' AS FLOAT64)) AS samp1_mean,
  STDDEV(CAST('$2' AS FLOAT64)) AS samp1_std,
  (SELECT AVG(CAST('$2' AS FLOAT64)) FROM `'$3'.'$4'.'$5'`) AS pop_mean,
  (SELECT STDDEV(CAST('$2' AS FLOAT64)) FROM `'$3'.'$4'.'$5'`) AS pop_std,
  CAST(COUNT(*) AS INT64) AS samp1_len 
  FROM `'$3'.'$4'.'$5'`
  GROUP BY '$6'
  )
  SELECT '$6',
  z_statistic_ONE_SAMPLE(samp1_mean, pop_mean, pop_std, samp1_len) AS z_critical_onesample,
  t_statistic_ONE_SAMPLE(samp1_mean, pop_mean, samp1_std, samp1_len) AS t_critical_onesample,
  avg_samp1_VAR,
  samp1_len AS df_sample_number
  FROM shorttab2;'

	echo "Mean numerical feature value:"
	bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
    'SELECT AVG('$2') FROM `'$3'.'$4'.'$5'`'


}


# ---------------------------


# T-test: https://www.statology.org/t-score-p-value-calculator/
# Z-score : https://www.socscistatistics.com/pvalues/normaldistribution.aspx
ONE_SAMPLE_TESTS_t_and_zstatistic_of_CATfeat_perCategory(){

    # [0] z_statistic_ONE_SAMPLE : Comparing the sample population mean with the population mean (large sample size populations)
    # [1] t_statistic_ONE_SAMPLE : Comparing the sample population mean with the population mean (small sample size populations)
	
    # Inputs:
    # $1 = location
    # $2 = PROJECT_ID
    # $3 = dataset_name
    # $4 = TABLE_name
    # $5 = category_FEAT_name (CATEGORICAL feature)
	
    # Have to change it manually for now **** the function does not accept variables, only hard-coded text
    # samp1_FEAT_name (CATEGORICAL feature per category to test statistical significance with the mean)
	
    # Add the transformed feature to the table
    export transformed_FEAT=$(echo "transformed_FEAT")

	bq query \
            --location=$1 \
            --destination_table $PROJECT_ID:$dataset_name.temp_table \
            --allow_large_results \
            --use_legacy_sql=false \
    'SELECT *, ML.LABEL_ENCODER(gender) OVER () AS '$transformed_FEAT'
FROM `'$2'.'$3'.'$4'`;'


    # echo "Delete TABLE_name"
    # Delete TABLE_name because it is now in temp_table
    bq --location=$1 rm -f -t $2:$3.$4


    # https://cloud.google.com/bigquery/docs/managing-tables#renaming-table
    # echo "copy table temp_table to TABLE_name"
    bq --location=$1 cp \
    -a -f -n \
    $2:$3.temp_table \
    $2:$3.$4
    
    
    # Delete temp_table because it is now in TABLE_name
    bq --location=$1 rm -f -t $2:$3.temp_table
    

    ONE_SAMPLE_TESTS_t_and_zstatistic_of_NUMfeat_perCategory $1 $transformed_FEAT $2 $3 $4 $5
          

    echo "Mean numerical feature value:"
    bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
    'SELECT AVG('$transformed_FEAT') FROM `'$3'.'$4'.'$5'`'
}


# ---------------------------


# Z-score : https://www.socscistatistics.com/pvalues/normaldistribution.aspx
ONE_SAMPLE_TESTS_zstatistic_per_row(){

    # [0] z_statistic_ONE_SAMPLE : Comparing the sample population mean with the population mean (large sample size populations)
    
    # Ideal for the case for a list of probabilities for an event occurence, and one would like to know which probablistic events are statistically significant to occur with respect to the other events.
    # Note that the length is set to one, thus the t and z-statistic will be computed per row.

    # Inputs:
    # $1 = location
    # $2 = prob_perc
    # $3 = PROJECT_ID
    # $4 = dataset_name
    # $5 = TABLE_name_probcount
    
    
bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
    'CREATE TEMP FUNCTION z_statistic_ONE_SAMPLE(samp1_mean FLOAT64, pop_mean FLOAT64, pop_std FLOAT64, samp1_len INT64)
AS (
  (samp1_mean - pop_mean)/(pop_std/SQRT(samp1_len))
  ); 
   
  
  WITH shorttab2 AS
    (
  SELECT *, 
  (row_num*0)+(SELECT AVG(CAST('$2' AS FLOAT64)) FROM `'$3'.'$4'.'$5'`) AS pop_mean,
  (row_num*0)+(SELECT STDDEV(CAST('$2' AS FLOAT64)) FROM `'$3'.'$4'.'$5'`) AS pop_std
    FROM `'$3'.'$4'.'$5'`
  )
    SELECT *, 
    z_statistic_ONE_SAMPLE('$2', pop_mean, pop_std, 1) AS z_critical_onesample
    FROM shorttab2'
    
    
    # Clean-up and delete old table
    bq rm -t $PROJECT_ID:$dataset_name.$TABLE_name_probcount
}


# ---------------------------


# T-test: https://www.statology.org/t-score-p-value-calculator/
TWO_SAMPLE_TESTS_zstatistic_perbinarycategory(){

    # Inputs:
    # $1 = location
    # $2 = samp1_FEAT_name
    # $3 = PROJECT_ID
    # $4 = dataset_name
    # $5 = TABLE_name
    # $6 = category_FEAT_name
    # $7 = category_FEAT_name_VAR1
    # $8 = category_FEAT_name_VAR2
    
    # Z_statistic_TWO_SAMPLE : Comparing the the means of two sample populations
    
	bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
    'CREATE TEMP FUNCTION z_statistic_TWO_SAMPLE(samp1_mean FLOAT64, samp2_mean FLOAT64, samp1_std FLOAT64, samp2_std FLOAT64, samp1_len INT64, samp2_len INT64)
AS (
    (samp1_mean - samp2_mean)/SQRT( ((samp1_std*samp1_std)/samp1_len) + ((samp2_std*samp2_std)/samp2_len))
    );
    
    
    WITH shorttab2 AS
(
  SELECT
  (SELECT AVG(CAST('$2' AS FLOAT64)) FROM `'$3'.'$4'.'$5'` WHERE '$6'='$7') AS samp1_mean,
  (SELECT AVG(CAST('$2' AS FLOAT64)) FROM `'$3'.'$4'.'$5'` WHERE '$6'='$8') AS samp2_mean,
  (SELECT STDDEV(CAST('$2' AS FLOAT64)) FROM `'$3'.'$4'.'$5'` WHERE '$6'='$7') AS samp1_std,
  (SELECT STDDEV(CAST('$2' AS FLOAT64)) FROM `'$3'.'$4'.'$5'` WHERE '$6'='$8') AS samp2_std,
  (SELECT CAST(COUNT(*) AS INT64) FROM `'$3'.'$4'.'$5'` WHERE '$6'='$7')  AS samp1_len,
  (SELECT CAST(COUNT(*) AS INT64) FROM `'$3'.'$4'.'$5'` WHERE '$6'='$8')  AS samp2_len
  FROM `'$3'.'$4'.'$5'`
  WHERE '$2' IS NOT NULL AND '$6' IS NOT NULL
  )
  SELECT
  samp1_mean,
  samp2_mean,
  samp1_std,
  samp2_std,
  samp1_len,
  samp2_len,
  z_statistic_TWO_SAMPLE(samp1_mean, samp2_mean, samp1_std, samp2_std, samp1_len, samp2_len) AS z_critical_twosample,
  FROM shorttab2
  LIMIT 1
  ;'
}

# ---------------------------

# T-test: https://www.statology.org/t-score-p-value-calculator/
TWO_SAMPLE_TESTS_zstatistic(){

    # Inputs:
    # $1 = location
    # $2 = samp1_FEAT_name
    # $3 = samp2_FEAT_name
    # $4 = PROJECT_ID
    # $5 = dataset_name
    # $6 = TABLE_name
    
    # Z_statistic_TWO_SAMPLE : Comparing the the means of two sample populations
    
	bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
    'CREATE TEMP FUNCTION z_statistic_TWO_SAMPLE(samp1_mean FLOAT64, samp2_mean FLOAT64, samp1_std FLOAT64, samp2_std FLOAT64, samp1_len INT64, samp2_len INT64)
AS (
    (samp1_mean - samp2_mean)/SQRT( ((samp1_std*samp1_std)/samp1_len) + ((samp2_std*samp2_std)/samp2_len))
    );
    
    
    WITH shorttab2 AS
(
  SELECT
  (SELECT AVG(CAST('$2' AS FLOAT64)) FROM `'$4'.'$5'.'$6'`) AS samp1_mean,
  (SELECT AVG(CAST('$3' AS FLOAT64)) FROM `'$4'.'$5'.'$6'`) AS samp2_mean,
  (SELECT STDDEV(CAST('$2' AS FLOAT64)) FROM `'$4'.'$5'.'$6'`) AS samp1_std,
  (SELECT STDDEV(CAST('$3' AS FLOAT64)) FROM `'$4'.'$5'.'$6'`) AS samp2_std,
  (SELECT CAST(COUNT(*) AS INT64) FROM `'$4'.'$5'.'$6'`)  AS samp1_len,
  (SELECT CAST(COUNT(*) AS INT64) FROM `'$4'.'$5'.'$6'`)  AS samp2_len
  FROM `'$4'.'$5'.'$6'`
  )
  SELECT
  samp1_mean,
  samp2_mean,
  samp1_std,
  samp2_std,
  samp1_len,
  samp2_len,
  z_statistic_TWO_SAMPLE(samp1_mean, samp2_mean, samp1_std, samp2_std, samp1_len, samp2_len) AS z_critical_twosample,
  FROM shorttab2
  LIMIT 1
  ;'
}


# ---------------------------

# Calculate the p-value per z-statistic or t-statistic


# [Step 0] Calculate the t-statistic/z-score vector which is the CDF
# t_OR_Z = ((df_samp1 - df_samp2) - (df_samp1_mean - df_samp2_mean)) / sqrt( ((df_samp1_std**2)/df_samp1_len) + ((df_samp2_std**2)/df_samp2_len) )

# -- OUTPUT: t_OR_Z


# [Step 1] Calculate the  pdf (the normal distribution OR the probability density function)

# Calculate the pdf (the normal distribution OR the probability density function)
# pdf0 = ((1/(np.sqrt(2*math.pi)*t_OR_Z.std())))*np.exp(-((t_OR_Z - t_OR_Z.mean())**2)/(2*t_OR_Z.std()**2))

# Calculate the significance value (p-value)
# p_value = np.sum([pdf0[ind] for ind, i in enumerate(t_Z_vec) if abs(i) > abs(t_Z_critical)])

# -- OUTPUT: p_value


# ---------------------------


# ---------------------------



# ---------------------------


# ---------------------------


# ---------------------------

VIEW_the_columns_of_a_table(){

    # Inputs:
    # $1 = location
    # $2 = PROJECT_ID
    # $3 = dataset_name
    # $4 = TABLE_name
    
    # List only the column names
    bq query \
    --location=$1 \
    --allow_large_results \
    --use_legacy_sql=false \
    "SELECT column_name, data_type
FROM $2.$3.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME='$4';"

}




# ---------------------------
# Handmade Queries
# ---------------------------
join_two_tables(){
	
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

}


# ---------------------------

CLEAN_TABLE_bikeshare_full_clean0(){
	
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

}


# ---------------------------

CLEAN_TABLE_bikeshare_full_clean1(){
	
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
     FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name_clean_prev'`
     WHERE member_casual IS NOT NULL OR rideable_type IS NOT NULL OR gender = "Male" OR gender = "Female"
     )
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
     FROM temptable
     WHERE member_casual_T0 IS NOT NULL OR rideable_type IS NOT NULL OR gender = "Male" OR gender = "Female";'

}

#  OR ltrim(rtrim(gender)) != "" OR 

# ---------------------------

join_multiple_tables(){
	
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
            T1.birthyear FROM `'$PROJECT_ID'.'$dataset_name'.dailyActivity_merged` AS T0
FULL JOIN `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name1'` AS T1 ON T0.Id = T1.Id;'   

# Try 0: More unique identifier => problem: usertype, gender, birthyear are NULL for member_casual, rideable_type, which means that the Tables just do not align. Even if I stacked the tables and made a common header they would STILL not ALIGN. problem solved, there is no corresponding data.
# FULL JOIN `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name1'` AS T1 ON T0.ride_id = T1.trip_id
# JOIN ON ride_id=trip_id DOES NOTHING because TABLE0 (member_casual, rideable_type) does not align with TABLE1 (usertype, gender, birthyear)

# Try 1: Less unique identifier (does not work)
# FULL JOIN `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name1'` AS T1 ON T0.start_station_id = T1.start_station_id
# FULL JOIN `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name1'` AS T1 ON T0.end_station_id = T1.end_station_id
# FULL JOIN `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name1'` AS T1 ON T0.ride_id = T1.bikeid

}

# ---------------------------


# ---------------------------


# ---------------------------


# ---------------------------

# ---------------------------


# ---------------------------
# Functions END
# ---------------------------








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










# ---------------------------------------------




# -------------------------
# Join the two tables 
# -------------------------
# join_two_tables




# -------------------------
# View the tables in the dataset
# -------------------------
# bq --location=$location ls $PROJECT_ID:$dataset_name




# -------------------------
# View the columns in a TABLE
# -------------------------
# export TABLE_name=$(echo "bikeshare_full")
# export TABLE_name=$(echo "bikeshare_full_clean0")
# export TABLE_name=$(echo "bikeshare_full_clean1")
# VIEW_the_columns_of_a_table $location $PROJECT_ID $dataset_name $TABLE_name 




# -------------------------
# CLEAN TABLE bikeshare_full_clean0 :  Identify the main features for the analysis
# -------------------------
# CLEAN_TABLE_bikeshare_full_clean0



# -------------------------
# CLEAN TABLE bikeshare_full_clean1
# -------------------------
# Delete TABLE bikeshare_full_clean1
# bq rm -t $PROJECT_ID:$dataset_name.bikeshare_full_clean1

# Create TABLE bikeshare_full_clean1
# CLEAN_TABLE_bikeshare_full_clean1




# ---------------------------------------------
# Hypothesis Testing
# ---------------------------------------------

# TYPE A RESULTS : probability of a categorical event happening 

# Additive rule of probability: P(A or B) = P(A) + P(B) - P(A and B)
# ie: the probablity of a ([casual user being female] OR [casual user being male]) AND [casual user using an electric_bike]
# (Reponse)  (0.004766284784788248 + 0.007837291891818123) * 0.08795399798808465

# Multiplicative rule of probability: P(A and B) = P(A) * P(B)
# ie: the probablity of a [casual user being female] AND [casual user using an electric_bike]
# (Reponse)  0.004766284784788248 * 0.08795399798808465


# Statistical significance of probablistic count for CATEGORICAL features
# *** NOT AUTOMATED, but written out *** 
export val=$(echo "X0")

if [[ $val == "X0" ]]
then 
    
    export TABLE_name=$(echo "bikeshare_full_clean1")
    
    export TABLE_name_probcount=$(echo "bikeshare_full_clean1_CATprobcount")

    # Calculation of percentage/probability of occurence across all samples
    bq query \
            --location=$location \
            --destination_table $PROJECT_ID:$dataset_name.$TABLE_name_probcount \
            --allow_large_results \
            --use_legacy_sql=false \
    'SELECT ROW_NUMBER() OVER(ORDER BY member_casual) AS row_num,
    member_casual, 
    rideable_type, 
    gender, 
    COUNT(*)/(SELECT COUNT(*) FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`) AS prob_perc
     FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`
     GROUP BY member_casual, rideable_type, gender
     ORDER BY member_casual, rideable_type, gender;'


    # Is the probability occurence (percentage) per group across all samples statistically significant?
    # Could improve this and add the p-value function as a new column
    export prob_perc=$(echo "prob_perc")  # name of numerical column to find z-statistic values per row
    ONE_SAMPLE_TESTS_zstatistic_per_row $location $prob_perc $PROJECT_ID $dataset_name $TABLE_name_probcount
  
    
fi



# ---------------------------------------------


# Statistical significance of probablistic count for NUMERICAL features per BIN_NUMBER
# *** NOT AUTOMATED, but written out *** 
export val=$(echo "X1")

if [[ $val == "X0" ]]
then 
	export TABLE_name=$(echo "bikeshare_full_clean1")
	export TABLE_name_probcount=$(echo "TABLE_name_probcount")

	# Calculation of percentage/probability of occurence of a numerical feature (workout_minutes) for a bin_number [ie: days (weekday=5, weekend=2)] across all samples
    bq query \
            --location=$location \
            --destination_table $PROJECT_ID:$dataset_name.$TABLE_name_probcount \
            --allow_large_results \
            --use_legacy_sql=false \
    'WITH tab2 AS
(
  SELECT *, 
  (SELECT SUM(workout_minutes)/bin_number FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'` WHERE wday ="weekend") AS pop_weekend 
  FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`)
)
SELECT lifestyle, wday, (SUM(workout_minutes)/bin_number)/AVG(pop_weekend) AS prob_perc 
FROM tab2
GROUP BY lifestyle, wday
ORDER BY wday, lifestyle;'


    # Is the probability occurence (percentage) per group across all samples statistically significant?
    # Could improve this and add the p-value function as a new column
    export prob_perc=$(echo "prob_perc")  # name of numerical column to find z-statistic values per row
    ONE_SAMPLE_TESTS_zstatistic_per_row $location $prob_perc $PROJECT_ID $dataset_name $TABLE_name_probcount

fi






# ---------------------------------------------


# TYPE B RESULTS : statistial probability of numerical features being different for categorical events

# ---------------------------------------------
# Run NUMERICAL FEATURES ONE SAMPLE TESTS (AUTOMATED)
# ---------------------------------------------
export val=$(echo "X1")

if [[ $val == "X0" ]]
then 
	export TABLE_name=$(echo "bikeshare_full_clean1")
	
	declare -a NUM_FEATS=('trip_distance' 'trip_time' 'birthyear_INT');
	
	# ********* CHANGE *********
	echo "Categorical feature:"
	export category_FEAT_name=$(echo "member_casual")
	echo $category_FEAT_name
	# ********* CHANGE *********
	
	for samp1_FEAT_name in "${NUM_FEATS[@]}"
	do	
		echo "Numerical feature:"
		echo $samp1_FEAT_name
   		ONE_SAMPLE_TESTS_t_and_zstatistic_of_NUMfeat_perCategory $location $samp1_FEAT_name $PROJECT_ID $dataset_name $TABLE_name $category_FEAT_name
	done
	
fi
# ---------------------------------------------




# ---------------------------------------------
# Run CATEGORICAL FEATURES ONE SAMPLE TESTS (NOT AUTOMATED)
# ---------------------------------------------
export val=$(echo "X1")

if [[ $val == "X0" ]]
then 
	export TABLE_name=$(echo "bikeshare_full_clean1")
	
	# ********* CHANGE *********
	echo "Categorical feature:"
	export category_FEAT_name=$(echo "member_casual")
	echo $category_FEAT_name
	
	echo "Transform categorical feature into a Numerical feature:"
	echo "gender"  # Need to copy paste into function
	ONE_SAMPLE_TESTS_t_and_zstatistic_of_CATfeat_perCategory $location $PROJECT_ID $dataset_name $TABLE_name $category_FEAT_name
	# ********* CHANGE *********
	
	# Confirm the numerical values with the categories
	bq query \
            --location=$location \
            --allow_large_results \
            --use_legacy_sql=false \
    'SELECT gender, transformed_FEAT, COUNT(*)
     FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`
     GROUP BY gender, transformed_FEAT;'
fi
# ---------------------------------------------





# ---------------------------------------------
# Run NUMERICAL FEATURES TWO SAMPLE TEST (AUTOMATED)
# ---------------------------------------------
export val=$(echo "X1")

if [[ $val == "X0" ]]
then 
	export TABLE_name=$(echo "bikeshare_full_clean1")
	
	declare -a NUM_FEATS=('trip_distance' 'trip_time' 'birthyear_INT');
	
	# ********* CHANGE *********
	echo "Categorical feature:"
	export category_FEAT_name=$(echo "member_casual")
	export category_FEAT_name_VAR1=$(echo "'member'")
	export category_FEAT_name_VAR2=$(echo "'casual'")
	echo $category_FEAT_name" where variables are "$category_FEAT_name_VAR1" and "$category_FEAT_name_VAR2
	# ********* CHANGE *********
	
	for samp1_FEAT_name in "${NUM_FEATS[@]}"
	do	
		echo "Numerical feature:"
		echo $samp1_FEAT_name
   		TWO_SAMPLE_TESTS_zstatistic_perbinarycategory $location $samp1_FEAT_name $PROJECT_ID $dataset_name $TABLE_name $category_FEAT_name $category_FEAT_name_VAR1 $category_FEAT_name_VAR2
	done

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
    # bq rm -t $PROJECT_ID:$dataset_name.twosample_table
    
fi


# ---------------------------------------------

