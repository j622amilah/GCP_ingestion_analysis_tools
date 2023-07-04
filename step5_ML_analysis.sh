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
test_train_split_equal_class_samples(){

    # Inputs:
    # $1 = location
    # $2 = PROJECT_ID
    # $3 = dataset_name
    # $4 = label_name
    # $5 = train_split
    # $6 = TRAIN_TABLE_name
    # $7 = TESTING_TABLE_name
    # $8 = TABLE_name
    
   
    bq rm -t $PROJECT_ID:$dataset_name.$6
    bq rm -t $PROJECT_ID:$dataset_name.$7
    
    # Make a random number label per class : OK
     bq query \
            --location=$1 \
            --destination_table $2:$3.table_train_test_split \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT ROW_NUMBER() OVER(PARTITION BY member_casual ORDER BY RAND() DESC) AS num_row_per_class, *
            FROM `'$2'.'$3'.'$8'`;'  

     echo "Look at class balance: Take data for the minimum class count for now"
     # Look at class balance
     bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT '$4', COUNT(*) FROM `'$2'.'$3'.table_train_test_split` 
            WHERE num_row_per_class IS NOT NULL GROUP BY '$4' ORDER BY '$4' ASC;'

     # Data could be cut to preserve class balance using the WHERE statement
     bq query \
            --location=$1 \
            --destination_table $2:$3.$6 \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT *
            FROM `'$2'.'$3'.table_train_test_split`
            WHERE num_row_per_class < '$5'*(SELECT COUNT(*) FROM `'$2'.'$3'.table_train_test_split` WHERE num_row_per_class IS NOT NULL GROUP BY '$4' ORDER BY '$4' ASC LIMIT 1);' 
   
     bq query \
            --location=$1 \
            --destination_table $2:$3.$7 \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT *
            FROM `'$2'.'$3'.table_train_test_split`
            WHERE num_row_per_class > '$5'*(SELECT COUNT(*) FROM `'$2'.'$3'.table_train_test_split` WHERE num_row_per_class IS NOT NULL GROUP BY '$4' ORDER BY '$4' ASC LIMIT 1) AND num_row_per_class < (SELECT COUNT(*) FROM `'$2'.'$3'.table_train_test_split` WHERE num_row_per_class IS NOT NULL GROUP BY '$4' ORDER BY '$4' ASC LIMIT 1);'  
     
     
     
     # Confirmation of rows in the final tables
     echo "Original table rows: "
     bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT COUNT(*)
            FROM `'$2'.'$3'.'$8'`;'  
            
     echo "table_train_test_split table rows: "
     bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT COUNT(*)
            FROM `'$2'.'$3'.table_train_test_split`;'  
            
     echo "TRAIN_TABLE_name table rows: train_split*(min_class_count*num_of_classes)"
     bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT COUNT(*)
            FROM `'$2'.'$3'.'$6'`;'  
     
     echo "TEST_TABLE_name table rows: (1-train_split)*(min_class_count*num_of_classes) "
     bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT COUNT(*)
            FROM `'$2'.'$3'.'$7'`;'  
            
     # Delete TABLE table_train_test_split
     bq rm -t $PROJECT_ID:$dataset_name.table_train_test_split
     
}


# ---------------------------


test_train_split_NONequal_class_samples(){

    # Inputs:
    # $1 = location
    # $2 = PROJECT_ID
    # $3 = dataset_name
    # $4 = label_name
    # $5 = train_split
    # $6 = TRAIN_TABLE_name
    # $7 = TESTING_TABLE_name
    # $8 = TABLE_name
    
   
    bq rm -t $PROJECT_ID:$dataset_name.$6
    bq rm -t $PROJECT_ID:$dataset_name.$7
    
    # Make a random number label per class : OK
     bq query \
            --location=$1 \
            --destination_table $2:$3.table_train_test_split \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT ROW_NUMBER() OVER(PARTITION BY member_casual ORDER BY RAND() DESC) AS num_row_per_class, *
            FROM `'$2'.'$3'.'$8'`;'  

     echo "Look at class balance: Take data for the minimum class count for now"
     # Look at class balance
     bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT '$4', COUNT(*) FROM `'$2'.'$3'.table_train_test_split` 
            WHERE num_row_per_class IS NOT NULL GROUP BY '$4' ORDER BY '$4' ASC;'


     # Test dataset has equal number of class values
     bq query \
            --location=$1 \
            --destination_table $2:$3.$7 \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT *
            FROM `'$2'.'$3'.table_train_test_split`
            WHERE num_row_per_class < (1-'$5')*(SELECT COUNT(*) FROM `'$2'.'$3'.table_train_test_split` WHERE num_row_per_class IS NOT NULL GROUP BY '$4' ORDER BY '$4' ASC LIMIT 1);' 



     # Training dataset has the rest of the data regardless of class balance
     bq query \
            --location=$1 \
            --destination_table $2:$3.$6 \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT *
            FROM `'$2'.'$3'.table_train_test_split`
            WHERE num_row_per_class > (1-'$5')*(SELECT COUNT(*) FROM `'$2'.'$3'.table_train_test_split` WHERE num_row_per_class IS NOT NULL GROUP BY '$4' ORDER BY '$4' ASC LIMIT 1);' 
   
     
     # Confirmation of rows in the final tables
     echo "Original table rows: "
     bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT COUNT(*)
            FROM `'$2'.'$3'.'$8'`;'  
            
     echo "table_train_test_split table rows: "
     bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT COUNT(*)
            FROM `'$2'.'$3'.table_train_test_split`;'  
            
     echo "TRAIN_TABLE_name table rows: train_split*(min_class_count*num_of_classes)"
     bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT COUNT(*)
            FROM `'$2'.'$3'.'$6'`;'  
     
     echo "TEST_TABLE_name table rows: (1-train_split)*(min_class_count*num_of_classes) "
     bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT COUNT(*)
            FROM `'$2'.'$3'.'$7'`;'  
            
     # Delete TABLE table_train_test_split
     bq rm -t $PROJECT_ID:$dataset_name.table_train_test_split
     
}

# ---------------------------



logistic_regression(){

    # Inputs:
    # $1 = location
    # $2 = PROJECT_ID
    # $3 = dataset_name
    # $4 = TRAIN_TABLE_name
    # $5 = TESTING_TABLE_name
    # $6 = MODEL_name
	

	# member_casual AS label
   
     bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
    'CREATE MODEL '$3'.'$6'
    OPTIONS(model_type="logistic_reg", LEARN_RATE_STRATEGY="CONSTANT", learn_rate=0.01, l2_reg=0.1, max_iterations=50, early_stop=TRUE, MIN_REL_PROGRESS=0.001, WARM_START=FALSE, CATEGORY_ENCODING_METHOD="DUMMY_ENCODING") AS 
    SELECT 
    trip_time,
    (CASE WHEN rideable_type="electric_bike" then 1 WHEN rideable_type="classic_bike" then 2 WHEN rideable_type="docked_bike" then 3 WHEN rideable_type IS NULL then 3 end) AS rideable_type_INTfill, 
    IF(member_casual = "member", 0, 1) AS label
    FROM `'$2'.'$3'.'$4'`'
   
   
   
     bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
    'SELECT *
    FROM ML.EVALUATE(
    	MODEL '$3'.'$6', 
    (
    SELECT 
       trip_time,
    (CASE WHEN rideable_type="electric_bike" then 1 WHEN rideable_type="classic_bike" then 2 WHEN rideable_type="docked_bike" then 3 WHEN rideable_type IS NULL then 3 end) AS rideable_type_INTfill,  
    IF(member_casual = "member", 0, 1) AS label
    FROM `'$2'.'$3'.'$4'`
    )
    	)'
 
 
 
 # Prédire des nouvelle etiquettes
    bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
    'SELECT *
    FROM ML.PREDICT(
    MODEL '$3'.'$6',  
    (
    SELECT 
    trip_time,
    (CASE WHEN rideable_type="electric_bike" then 1 WHEN rideable_type="classic_bike" then 2 WHEN rideable_type="docked_bike" then 3 WHEN rideable_type IS NULL then 3 end) AS rideable_type_INTfill,  
    IF(member_casual = "member", 0, 1) AS label
    FROM `'$2'.'$3'.'$5'` LIMIT 10
    )
    	)'
}


# ---------------------------



kmeans(){

    # Inputs:
    # $1 = location
    # $2 = PROJECT_ID
    # $3 = dataset_name
    # $4 = TRAIN_TABLE_name
    # $5 = TESTING_TABLE_name
    # $6 = MODEL_name
	
    # Bigquery needs numerical features and labels

     bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
    'CREATE MODEL '$3'.'$6'
    OPTIONS(model_type="KMEANS", NUM_CLUSTERS=2, KMEANS_INIT_METHOD="KMEANS++", MAX_ITERATIONS=50, early_stop=TRUE, MIN_REL_PROGRESS=0.001, WARM_START=FALSE) AS 
    SELECT 
    trip_time,
    (CASE WHEN rideable_type="electric_bike" then 1 WHEN rideable_type="classic_bike" then 2 WHEN rideable_type="docked_bike" then 3 WHEN rideable_type IS NULL then 3 end) AS rideable_type_INTfill, 
    IF(member_casual = "member", 0, 1) AS label
    FROM `'$2'.'$3'.'$4'`'
   
   
   # Evaluate
     bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
    'SELECT *
    FROM ML.EVALUATE(
    	MODEL '$3'.'$6', 
    (
    SELECT 
       trip_time,
    (CASE WHEN rideable_type="electric_bike" then 1 WHEN rideable_type="classic_bike" then 2 WHEN rideable_type="docked_bike" then 3 WHEN rideable_type IS NULL then 3 end) AS rideable_type_INTfill,  
    IF(member_casual = "member", 0, 1) AS label
    FROM `'$2'.'$3'.'$4'`
    )
    	)'
 
 
 
 # Prédire des nouvelle etiquettes
    bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
    'SELECT *
    FROM ML.PREDICT(
    MODEL '$3'.'$6',  
    (
    SELECT 
    trip_time,
    (CASE WHEN rideable_type="electric_bike" then 1 WHEN rideable_type="classic_bike" then 2 WHEN rideable_type="docked_bike" then 3 WHEN rideable_type IS NULL then 3 end) AS rideable_type_INTfill,  
    IF(member_casual = "member", 0, 1) AS label
    FROM `'$2'.'$3'.'$5'` LIMIT 10
    )
    	)'
     
}


# ---------------------------


# Unsupported dataset location europe-west9 for model BOOSTED_TREE_CLASSIFIER
decision_tree(){

    # Inputs:
    # $1 = location
    # $2 = PROJECT_ID
    # $3 = dataset_name
    # $4 = TRAIN_TABLE_name
    # $5 = MODEL_name
	
	
	bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
    'CREATE OR REPLACE MODEL '$3'.'$5'
    OPTIONS(model_type="BOOSTED_TREE_CLASSIFIER", l2_reg = 0.01, num_parallel_tree = 8, max_tree_depth = 10, max_iterations=50, early_stop=TRUE, MIN_REL_PROGRESS=0.001, CATEGORY_ENCODING_METHOD="LABEL_ENCODING") AS 
    SELECT 
    trip_time, 
    IF(birthyear_INT IS NULL, 1981, birthyear_INT) AS birthyear_INTfill,
    (CASE WHEN rideable_type="electric_bike" then 1 WHEN rideable_type="classic_bike" then 2 WHEN rideable_type="docked_bike" then 3 WHEN rideable_type IS NULL then 3 end) AS rideable_type_INTfill, 
    IF(member_casual = "member", 0, 1) AS label
    FROM `'$2'.'$3'.'$4'`
   '

}


# ---------------------------



random_forest(){

    # Inputs:
    # $1 = location
    # $2 = PROJECT_ID
    # $3 = dataset_name
    # $4 = TRAIN_TABLE_name
    # $5 = MODEL_name
	
	
	bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
    'CREATE OR REPLACE MODEL '$3'.'$5'
    OPTIONS(model_type="RANDOM_FOREST_CLASSIFIER", l2_reg = 0.01, num_parallel_tree=8, max_tree_depth = 10, max_iterations=50, early_stop=TRUE, MIN_REL_PROGRESS=0.001, INPUT_LABEL_COLS=["member_casual"]) AS 
    SELECT 
    trip_time, 
    IF(birthyear_INT IS NULL, 1981, birthyear_INT) AS birthyear_INTfill,
    (CASE WHEN rideable_type="electric_bike" then 1 WHEN rideable_type="classic_bike" then 2 WHEN rideable_type="docked_bike" then 3 WHEN rideable_type IS NULL then 3 end) AS rideable_type_INTfill
    FROM `'$2'.'$3'.'$4'`
   '

}


# ---------------------------



deep_neural_network(){

    # Inputs:
    # $1 = location
    # $2 = PROJECT_ID
    # $3 = dataset_name
    # $4 = TRAIN_TABLE_name
    # $5 = MODEL_name
	
     # Invalid OPTIMIZER 'adam'. Valid optimizer with L1/L2 reguralization specified are Adagrad, Ftrl and SGD.

	bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
    'CREATE MODEL '$2'.'$3'.'$5'
    OPTIONS(MODEL_TYPE="DNN_CLASSIFIER", INPUT_LABEL_COLS = ["member_casual"]) 
    AS SELECT 
    IF(birthyear_INT IS NULL, 1981, birthyear_INT) AS birthyear_INTfill,
    (CASE WHEN rideable_type="electric_bike" then 1 WHEN rideable_type="classic_bike" then 2 WHEN rideable_type="docked_bike" then 3 WHEN rideable_type IS NULL then 3 end) AS rideable_type_INTfill
    FROM `'$2'.'$3'.'$4'`;'
    
    # OPTIONS(model_type="DNN_CLASSIFIER", ACTIVATION_FN="RELU", HIDDEN_UNITS = [256, 128, 64], learn_rate=0.01, max_iterations=50, early_stop=TRUE, MIN_REL_PROGRESS=0.001, OPTIMIZER="ADAGRAD", l2_reg = 0.01, TF_VERSION="2.8.0", ENABLE_GLOBAL_EXPLAIN=TRUE, INPUT_LABEL_COLS = ["member_casual"]) AS 
    
     # 0.768128518349618
     # trip_time, birthyear_INT, rideable_type, IF(member_casual = "member", 0, 1) AS label
   
     # 0.77
     # trip_time, birthyear_INT, rideable_type, gender, fin_stsID, fin_esID, trip_distance, IF(member_casual = "member", 0, 1) AS label
     

}


# ---------------------------



autoML_model(){

    # Inputs:
    # $1 = location
    # $2 = PROJECT_ID
    # $3 = dataset_name
    # $4 = TRAIN_TABLE_name
    # $5 = MODEL_name
	
     # Invalid OPTIMIZER 'adam'. Valid optimizer with L1/L2 reguralization specified are Adagrad, Ftrl and SGD.

	bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
    'CREATE MODEL '$2'.'$3'.'$5'
    OPTIONS(MODEL_TYPE="AUTOML_CLASSIFIER", INPUT_LABEL_COLS = ["member_casual"], OPTIMIZATION_OBJECTIVE="MAXIMIZE_AU_ROC") 
    AS SELECT 
    trip_time, 
    IF(birthyear_INT IS NULL, 1981, birthyear_INT) AS birthyear_INTfill,
    (CASE WHEN rideable_type="electric_bike" then 1 WHEN rideable_type="classic_bike" then 2 WHEN rideable_type="docked_bike" then 3 WHEN rideable_type IS NULL then 3 end) AS rideable_type_INTfill
    FROM `'$2'.'$3'.'$4'`;'
    
    
     

}



# ---------------------------
# evaluate_the_model $location $PROJECT_ID $dataset_name $MODEL_name $TRAIN_TABLE_name
evaluate_the_model(){

    # EVALUATE expects that column 'label' is a STRING, but type was INT64
	
    # Inputs:
    # $1 = location
    # $2 = PROJECT_ID
    # $3 = dataset_name
    # $4 = MODEL_name
    # $5 = TRAIN_TABLE_name OR TESTING_TABLE_name
    
    # Print accuracy
    bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
    'SELECT *
    FROM ML.EVALUATE(MODEL '$3'.'$4', (
       SELECT 
       trip_time, 
       IF(birthyear_INT IS NULL, 1981, birthyear_INT) AS birthyear_INTfill,
    (CASE WHEN rideable_type="electric_bike" then 1 WHEN rideable_type="classic_bike" then 2 WHEN rideable_type="docked_bike" then 3 WHEN rideable_type IS NULL then 3 end) AS rideable_type_INTfill, 
       member_casual AS label
    FROM `'$2'.'$3'.'$5'`
    ))'
    
}

# ---------------------------

predict_model(){

    # Inputs:
    # $1 = location
    # $2 = PROJECT_ID
    # $3 = dataset_name
    # $4 = TABLE_name
    # $5 = MODEL_name
    
  
    # Prédire des nouvelle etiquettes
    bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
    'SELECT *
    FROM ML.PREDICT(
    MODEL '$3'.'$5',  
    (SELECT 
    trip_time, 
    IF(birthyear_INT IS NULL, 1981, birthyear_INT) AS birthyear_INTfill,
    (CASE WHEN rideable_type="electric_bike" then 1 WHEN rideable_type="classic_bike" then 2 WHEN rideable_type="docked_bike" then 3 WHEN rideable_type IS NULL then 3 end) AS rideable_type_INTfill, 
    IF(member_casual = "member", 0, 1) AS label
    FROM `'$2'.'$3'.'$4'` LIMIT 10)
    )'
}

# ---------------------------

get_trial_info(){

    # Inputs:
    # $1 = location
    # $2 = dataset_name
    # $3 = MODEL_name
    
    # Get trials information
    
    bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
    'SELECT *
    FROM ML.TRAINING_INFO(
    MODEL '$2'.'$3')'
}

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










# -------------------------
# View the tables in the dataset
# -------------------------
# bq --location=$location --max_rows_per_request=10000 ls $PROJECT_ID:$dataset_name



# -------------------------
# View the columns in a TABLE
# -------------------------
# export TABLE_name=$(echo "bikeshare_full")
# export TABLE_name=$(echo "bikeshare_full_clean0")
export TABLE_name=$(echo "bikeshare_full_clean1")
# VIEW_the_columns_of_a_table $location $PROJECT_ID $dataset_name $TABLE_name 




# -------------------------
# Test Train split
# -------------------------
export label_name=$(echo "member_casual")
export train_split=$(echo "0.75")
export TRAIN_TABLE_name=$(echo "TRAIN_TABLE_name")
export TESTING_TABLE_name=$(echo "TESTING_TABLE_name")

# Downsampling : Creates two class balanced randomized tables (TRAIN_TABLE_name, TEST_TABLE_name) from TABLE_name using the train_split value 
# test_train_split_equal_class_samples $location $PROJECT_ID $dataset_name $label_name $train_split $TRAIN_TABLE_name $TESTING_TABLE_name $TABLE_name

# Model weights : The test data is equally divided by class, but the train data contains the rest (the model weights can be used to account for class imbalance)
# test_train_split_NONequal_class_samples $location $PROJECT_ID $dataset_name $label_name $train_split $TRAIN_TABLE_name $TESTING_TABLE_name $TABLE_name



# -------------------------
# Train the model, evaluate, predict
# -------------------------
# Creates a model called MODEL_name in the dataset folder, that is trained using the TRAIN_TABLE_name table
# https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-create-automl
# detailed examples : https://cloud.google.com/bigquery/docs/logistic-regression-prediction


export val=$(echo "X1")

if [[ $val == "X0" ]]
then 
	echo "Logistic regression classification: "
	export MODEL_name=$(echo "logistic_reg_model")
	bq rm -f --model $PROJECT_ID:$dataset_name.$MODEL_name    # https://cloud.google.com/bigquery/docs/deleting-models
	logistic_regression $location $PROJECT_ID $dataset_name $TRAIN_TABLE_name $MODEL_name


	echo "K-means clustering: "
	export MODEL_name=$(echo "kmeans_model2")
	bq rm -f --model $PROJECT_ID:$dataset_name.$MODEL_name
	kmeans $location $PROJECT_ID $dataset_name $TRAIN_TABLE_name $TESTING_TABLE_name $MODEL_name


	# Problems: none of the models give a result except for logistic regression

	# echo "Decision tree classification: "
	# export MODEL_name=$(echo "decision_tree_model")
	# bq rm -f --model $PROJECT_ID:$dataset_name.$MODEL_name 
	# decision_tree $location $PROJECT_ID $dataset_name $TRAIN_TABLE_name $MODEL_name

	# echo "Deep neural network classification: "
	# export MODEL_name=$(echo "DNN_model")
	# bq rm -f --model $PROJECT_ID:$dataset_name.$MODEL_name 
	# deep_neural_network $location $PROJECT_ID $dataset_name $TRAIN_TABLE_name $MODEL_name

	# echo "AutoML classification: "
	# export MODEL_name=$(echo "AutoML_model")
	# bq rm -f --model $PROJECT_ID:$dataset_name.$MODEL_name 
	# autoML_model $location $PROJECT_ID $dataset_name $TRAIN_TABLE_name $MODEL_name

	# Print information about model
	# echo "Model information: "
	# get_trial_info $location $dataset_name $MODEL_name
fi


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
    # bq rm -t $PROJECT_ID:$dataset_name.out
    
fi


# ---------------------------------------------

