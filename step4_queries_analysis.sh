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
     
     # ride_id, rideable_type, started_at, ended_at, start_station_name, start_station_id, end_station_name, end_station_id, start_lat, start_lng, end_lat, end_lng, member_casual
     export TABLE_name0=$(echo "bikeshare_table0")
     
     # trip_id, starttime, stoptime, bikeid, tripduration, start_station_id, start_station_name, end_station_id, end_station_name, usertype, gender, birthyear
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
     
     export TABLE_name_join=$(echo "bikeshare_full")
     

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

fi


# ---------------------------------------------


export val=$(echo "X1")

if [[ $val == "X0" ]]
then 
	# Confirm that the Joined table was created
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
	export TABLE_name=$(echo "bikeshare_full")
    
    # List only the column names
    bq query \
    --location=$location \
    --allow_large_results \
    --use_legacy_sql=false \
    "SELECT column_name
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


# 1. How do annual members and casual riders use Cyclistic bikes diﬀerently?
# features : rideable_type, tripduration (need to fillin this column with ended_at-started)
export val=$(echo "X0")

if [[ $val == "X0" ]]
then 
    
    export TABLE_name=$(echo "bikeshare_full")
    
    
    bq query \
    --location=$location \
    --allow_large_results \
    --use_legacy_sql=false \
    'SELECT ended_at, stoptime FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`
    WHERE ended_at IS NOT NULL
    ORDER BY stoptime, ended_at DESC
    LIMIT 10;'
    
    
    
    

fi



# ---------------------------------------------

# 2. Why would casual riders buy Cyclistic annual memberships?

export val=$(echo "X1")

if [[ $val == "X0" ]]
then 
	echo ""

fi

# ---------------------------------------------

# 3. How can Cyclistic use digital media to inﬂuence casual riders to become members?

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
    
    bq rm -t $PROJECT_ID:$dataset_name.$TABLE_name_join
    
fi


# ---------------------------------------------

