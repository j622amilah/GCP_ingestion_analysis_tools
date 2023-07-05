#!/bin/bash

# cd /home/oem2/Documents/ONLINE_CLASSES/Spécialisation_Google_Data_Analytics/3_Google_Data_Analytics_Capstone_Complete_a_Case_Study/git2/automatic_GCP_ingestion

# ./step4_queries_analysis.sh



clear



# ---------------------------
# Handmade Query Functions
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

CLEAN_TABLE_exercise_full_clean0(){
	
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
    # export dataset_name=$(echo "google_analytics_exercise")
    # bq --location=$location mk $PROJECT_ID:$dataset_name

    # OR 

    # Use existing dataset
    export dataset_name=$(echo "google_analytics_exercise")

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
# Download data from datasource (Kaggle API)
# ---------------------------------------------
# https://www.kaggle.com/docs/api
# 0. go to kaggle - User icon - account - scroll down to API section - telechargez kaggle.json

# 1. bougez kaggle.json à la path du script
# mv /home/oem2/Documents/PROGRAMMING/kaggle.json /home/oem2/.kaggle/kaggle.json
# chmod 600 /home/oem2/.kaggle/kaggle.json

# Prepare folder
# cd /home/oem2/Documents/ONLINE_CLASSES/Spécialisation_Google_Data_Analytics/3_Google_Data_Analytics_Capstone_Complete_a_Case_Study
# mkdir exercise_casestudy
# cd /home/oem2/Documents/ONLINE_CLASSES/Spécialisation_Google_Data_Analytics/3_Google_Data_Analytics_Capstone_Complete_a_Case_Study/exercise_casestudy

# Run the API
# kaggle datasets download -d NAME_OF_DATASET




# ---------------------------------------------
# Download data from datasource (Kaggle API)
# ---------------------------------------------
# export path_folder_2_organize=$(echo "/home/oem2/Documents/ONLINE_CLASSES/Spécialisation_Google_Data_Analytics/3_Google_Data_Analytics_Capstone_Complete_a_Case_Study/exercise_casestudy")

# export ingestion_folder=$(echo "ingestion_folder_exercise")

# export path_outside_of_ingestion_folder=$(echo "/home/oem2/Documents/ONLINE_CLASSES/Spécialisation_Google_Data_Analytics/3_Google_Data_Analytics_Capstone_Complete_a_Case_Study")

# organize_zip_files_from_datasource_download $path_folder_2_organize $ingestion_folder $path_outside_of_ingestion_folder



# ---------------------------------------------
# Upload csv files from the PC to GCP
# ---------------------------------------------
# ******* CHANGE *******
# export cur_path=$(echo "/home/oem2/Documents/ONLINE_CLASSES/Spécialisation_Google_Data_Analytics/3_Google_Data_Analytics_Capstone_Complete_a_Case_Study/ingestion_folder_exercise/csvdata")
# ******* CHANGE *******

# echo "cur_path"
# echo $cur_path
    
# upload_csv_files $location $cur_path $dataset_name



# -------------------------
# Join the TABLES 
# -------------------------

# TO DO

# join_multiple_tables





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
# Initially Clean the TABLE :  Identify the main features for the analysis
# -------------------------
# CLEAN_TABLE_exercise_full_clean0





# ---------------------------------------------
# Statistical Analysis : Hypothesis Testing
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

