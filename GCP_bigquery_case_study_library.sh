#!/bin/bash


# ---------------------------
# Functions START
# ---------------------------

organize_zip_files_from_datasource_download(){
	
		
	# Automatically organizes a folder of zipped csv files into 3 folders: csvdata, remaining_files, zipdata
	
	
	# Inputs:
	# $1 = path_folder_2_organize (path where the zip files are located) (this should exist)
	# $2 = ingestion_folder (folder name to put the files into) = output_folder_NAME (one chooses this)
	# $3 = path_outside_of_ingestion_folder (path where the ingestion folder should be located) = output_folder_PATH (one chooses this)
	
	# Path inside of the ingestion folder
	export path_ingestion_folder=$(echo "$3/$2")
	# export path_ingestion_folder=$(echo "/home/oem2/Documents/ONLINE_COURS/Specialization_Google_Business_Intelligence_Certificat_Professionnel/case_study_Google_fiber/ingestion_folder")
	
	# ---------------------------------------------
	# Make ingestion folder and transfer files
	# ---------------------------------------------
	export val=$(echo "X0")

	if [[ $val == "X0" ]]
	then

	    mkdir $path_ingestion_folder
	    
	    cp -a $1/. $path_ingestion_folder
	    # OR
	    # cp -a /home/oem2/Documents/ONLINE_COURS/Specialization_Google_Business_Intelligence_Certificat_Professionnel/case_study_Google_fiber/data_download/. $path_ingestion_folder
	    
	fi

	# ---------------------------------------------




	# ---------------------------------------------
	# Unzip files
	# ---------------------------------------------
	export val=$(echo "X0")

	if [[ $val == "X0" ]]
	then 

	    # Unzip file options
	    # -f  freshen existing files, create none
	    # -n  never overwrite existing files
	    # -o  overwrite files WITHOUT prompting

	    cd $path_ingestion_folder

	    ls *.zip > arr
	    
	    for i in $(cat arr)
	    do
	       unzip -o $i
	    done

	    mkdir zipdata
	    mv *.zip zipdata

	    # Clean-up treatment files
	    rm arr
	    
	fi

	# ---------------------------------------------
	
	
	# Rename all the files so they do not have any spaces
	for f in *\ *; do mv "$f" "${f// /_}"; done
	
	
	# ---------------------------------------------
	# Convert xlsx files to csv
	# ---------------------------------------------
	export val=$(echo "X0")

	if [[ $val == "X0" ]]
	then 

	    # Unzip file options
	    # -f  freshen existing files, create none
	    # -n  never overwrite existing files
	    # -o  overwrite files WITHOUT prompting

	    cd $path_ingestion_folder
	    echo "Output: " 
	    echo $path_ingestion_folder
	    
	    # Read all file names into a file
	    ls *.xlsx > arr
	    
	    
	    for i in $(cat arr)
	    do
	       echo $i
	       libreoffice --headless --convert-to csv $i
	    done

	    # Clean-up treatment files
	    rm arr
	    
	fi

	# ---------------------------------------------




	# ---------------------------------------------
	# Secondary clean up of files
	# ---------------------------------------------
	export val=$(echo "X0")

	if [[ $val == "X0" ]]
	then 
	    
	    # get main path
	    # export cur_path=$(pwd)
	    # echo "cur_path:"
	    # echo $cur_path
	    
	    # Get path of folder to search
	    # export path_ingestion_folder=$(echo "${cur_path}/${folder_2_organize}")
	    # echo "path_ingestion_folder:"
	    # echo $path_ingestion_folder
	    # /home/oem2/Documents/COURS_ONLINE/Spécialisation_Google_Data_Analytics/3_Google_Data_Analytics_Capstone_Complete_a_Case_Study/bike_casestudy/dataORG
	    
	    # find folders inside of the folder to search
	    cd $path_ingestion_folder
	    
	    # write folder names in file
	    ls -d */ >> folder_list
	   
	    # move folder contents into data
	    # export i=$(echo "Divvy_Stations_Trips_2013/")
	    for i in $(cat folder_list)
	    do
	      export new_path=$(echo "${path_ingestion_folder}/${i}")
	      echo "new_path:"
	      echo $new_path
	      
	      cd $new_path
	      
	      # Save an array of values 
	      # remove the text folder_list2 from the file, then remove blank or empty lines
	      ls  | sed 's/folder_list2//g' | sed '/^$/d' >> folder_list2
	      
	      #echo "contents of folder_list2:"
	      for j in $(cat folder_list2)
	      do
		#echo $j
		export new_path2=$(echo "${new_path}${j}")
		#echo "new_path2:"
		#echo $new_path2
		mv $new_path2 $path_ingestion_folder 
	      done
	      
	      # delete folders
	      rm folder_list2
	      
	      cd $path_ingestion_folder
	      
	      rm -rf $i
	    done
	    
	    
	    rm folder_list
	   
	    # Recreate main folders
	    # --------------
	    # zipfile folder
	    mkdir zipdata
	    mv *.zip zipdata
	    # --------------
	    
	    # --------------
	    # csv folder
	    mkdir csvdata
	    mv *.csv csvdata
	    # --------------
	    
	    # --------------
	    # The rest in a folder
	    mkdir remaining_files
	    
	    find $path_ingestion_folder -maxdepth 1 -type f >> nondir_folder_list
	    
	    # remove the directory items from the file all_file_list
	    for i in $(cat nondir_folder_list)
	    do
	      mv $i remaining_files
	    done
	    
	    rm remaining_files/nondir_folder_list
	    # --------------

	fi

	# ---------------------------------------------

}


# ---------------------------------------------


upload_csv_files(){
	
    # Inputs:
    # $1 = location
    # $2 = cur_path
    # $3 = dataset_name


    cd $2
    
    
    if [ -f file_list_names ]; then
       rm file_list_names
    fi
  
    if [ -f table_list_names ]; then
       rm table_list_names
    fi
    
    # Get list of csv files : do not remove the .csv for bq load
    ls  | sed 's/file_list_names//g' | sed '/^$/d' >> file_list_names
    
    
    # Generic name of tables
    export Generic_CSV_FILENAME=$(cat file_list_names | head -n 1 | sed 's/.csv//g' | tr -d [0-9] | sed 's/-//g' | sed 's/  */ /g' | sed 's/^ *//g' | tr '[:upper:]' '[:lower:]' | sed -e '/[[:space:]]\+$/s///' | sed 's/[[:space:]]/_/g')
    echo "Generic_CSV_FILENAME: "
    echo $Generic_CSV_FILENAME 
    
    # Load CSV file into a BigQuery table FROM PC
    cnt=0
    for CSV_NAME in $(cat file_list_names)
    do
       echo "CSV_NAME: "
       echo $CSV_NAME 
       
       # -----------------------------
       # Edit csv file to prevent bq load errors : this section could get bigger
       # -----------------------------
       # Remove and from all csv files to prevent TIMESTAMP error : *** could be better ***
       # cat $CSV_NAME | sed 's///g' | sed 's///g' > temp_csv
       # mv temp_csv $CSV_NAME
       # -----------------------------
       
       # ******* CHANGE *******
       # 0. Enter the name of the table to upload to GCP
       
       # Table name choices:
       # a) Use the csv file names directly
       # remove the .csv from the filename
       export TABLE_name=$(echo $CSV_NAME | sed 's/.csv//g')
       
       # b) Use a common name for all the csv files, put a counter to distinguish each file : this is necessary for UNION-ing the tables
       # export TABLE_name=$(echo "${Generic_CSV_FILENAME}bday${cnt}")
       
       echo "TABLE_name: "
       echo $TABLE_name
       # ******* CHANGE *******
       
       # Need to save a list of TABLE_name, to do the UNION query next
       echo $TABLE_name >> table_list_names
       # OR
       # Save tables to a dataset folder dedicated to one thing, and use bq ls to get table names in next query
       
       # BigQuery error in load operation: Cannot determine table described
       # If you are are getting this error , it is an authentication and authorization issue, simply log out and log in again. e.g if you are using cloud shell – close it and reopen.
       
       # Upload with schema options: autodetect the schema fields
        bq load \
            --location=$1 \
            --source_format=CSV \
            --skip_leading_rows=1 \
            --autodetect \
            $3.$TABLE_name \
            ./$CSV_NAME
            
        cnt=$((cnt + 1))
       
    done

}


# ---------------------------------------------


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



# ---------------------------------------------


test_train_split_equal_class_samples(){

    # Inputs:
    # $1 = location
    # $2 = PROJECT_ID
    # $3 = dataset_name
    # $4 = label_name
    # $5 = train_split
    # $6 = TRAIN_TABLE_name
    # $7 = TESTING_TABLE_name
    # $8 = TABLE_name2
    
   
    bq rm -t $2:$3.$6
    bq rm -t $2:$3.$7
    
    # Make a random number label per class : OK
     bq query \
            --location=$1 \
            --destination_table $2:$3.table_train_test_split \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT ROW_NUMBER() OVER(PARTITION BY '$4' ORDER BY RAND() DESC) AS num_row_per_class, *
            FROM `'$2'.'$3'.'$8'`;'  

     echo "Look at class balance: Take data for the minimum class count for now"
     # Look at class balance
     bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT '$4', COUNT(*) FROM `'$2'.'$3'.table_train_test_split` 
            WHERE num_row_per_class IS NOT NULL GROUP BY '$4' ORDER BY COUNT(*) ASC;'

     # Data could be cut to preserve class balance using the WHERE statement
     bq query \
            --location=$1 \
            --destination_table $2:$3.$6 \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT *
            FROM `'$2'.'$3'.table_train_test_split`
            WHERE num_row_per_class < '$5'*(SELECT COUNT(*) FROM `'$2'.'$3'.table_train_test_split` WHERE num_row_per_class IS NOT NULL GROUP BY '$4' ORDER BY COUNT(*) ASC LIMIT 1);' 
   
     bq query \
            --location=$1 \
            --destination_table $2:$3.$7 \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT *
            FROM `'$2'.'$3'.table_train_test_split`
            WHERE num_row_per_class > '$5'*(SELECT COUNT(*) FROM `'$2'.'$3'.table_train_test_split` WHERE num_row_per_class IS NOT NULL GROUP BY '$4' ORDER BY COUNT(*) ASC LIMIT 1) AND num_row_per_class < (SELECT COUNT(*) FROM `'$2'.'$3'.table_train_test_split` WHERE num_row_per_class IS NOT NULL GROUP BY '$4' ORDER BY COUNT(*) ASC LIMIT 1);'  
     
     
     
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
     bq rm -t $2:$3.table_train_test_split
     
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
    # $8 = TABLE_name2
    
   
    bq rm -t $2:$3.$6
    bq rm -t $2:$3.$7
    
    # Make a random number label per class : OK
     bq query \
            --location=$1 \
            --destination_table $2:$3.table_train_test_split \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT ROW_NUMBER() OVER(PARTITION BY label ORDER BY RAND() DESC) AS num_row_per_class, *
            FROM `'$2'.'$3'.'$8'`;'  

     echo "Look at class balance: Take data for the minimum class count for now"
     # Look at class balance
     bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT '$4', COUNT(*) FROM `'$2'.'$3'.table_train_test_split` 
            WHERE num_row_per_class IS NOT NULL GROUP BY '$4' ORDER BY COUNT(*) ASC;'


     # Test dataset has equal number of class values
     bq query \
            --location=$1 \
            --destination_table $2:$3.$7 \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT *
            FROM `'$2'.'$3'.table_train_test_split`
            WHERE num_row_per_class < (1-'$5')*(SELECT COUNT(*) FROM `'$2'.'$3'.table_train_test_split` WHERE num_row_per_class IS NOT NULL GROUP BY '$4' ORDER BY COUNT(*) ASC LIMIT 1);' 



     # Training dataset has the rest of the data regardless of class balance
     bq query \
            --location=$1 \
            --destination_table $2:$3.$6 \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT *
            FROM `'$2'.'$3'.table_train_test_split`
            WHERE num_row_per_class > (1-'$5')*(SELECT COUNT(*) FROM `'$2'.'$3'.table_train_test_split` WHERE num_row_per_class IS NOT NULL GROUP BY '$4' ORDER BY COUNT(*) ASC LIMIT 1);' 
   
     
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
     bq rm -t $2:$3.table_train_test_split
     
}

# ---------------------------

evaluate_model(){

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
    'SELECT *
    FROM ML.EVALUATE(
    	MODEL '$3'.'$5', 
    (
    SELECT *
    FROM `'$2'.'$3'.'$4'`
    )
    	)'
}


# ---------------------------


predict_with_model(){

    # Inputs:
    # $1 = location
    # $2 = PROJECT_ID
    # $3 = dataset_name
    # $4 = TESTING_TABLE_name
    # $5 = MODEL_name
    # $6 = PREDICTED_results_TABLE_name
    
    # Ensure that the OUTPUT TABLE does not already exist, to prevent saving errors
    bq rm -t $2:$3.$6
    
    
    bq query \
            --location=$1 \
            --destination_table $2:$3.$6 \
            --allow_large_results \
            --use_legacy_sql=false \
    'SELECT *
    FROM ML.PREDICT(
    MODEL '$3'.'$5',  
    (
    SELECT *
    FROM `'$2'.'$3'.'$4'`
    )
    	)'

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
    # $7 = PREDICTED_results_TABLE_name
	
   
     bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
    'CREATE MODEL '$3'.'$6'
    OPTIONS(model_type="logistic_reg", LEARN_RATE_STRATEGY="CONSTANT", learn_rate=0.01, l2_reg=0.1, max_iterations=50, early_stop=TRUE, MIN_REL_PROGRESS=0.001, WARM_START=FALSE, CATEGORY_ENCODING_METHOD="DUMMY_ENCODING") AS 
    SELECT *
    FROM `'$2'.'$3'.'$4'`'
   
   # Evaluate
   evaluate_model $1 $2 $3 $4 $6
 
   # Prédire des nouvelle etiquettes
   predict_with_model $1 $2 $3 $5 $6 $7
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
    # $7 = PREDICTED_results_TABLE_name
    # $8 = NUM_CLUSTERS
	
    # Bigquery needs numerical features and labels
	
    # Ensure that the OUTPUT model does not already exist, to prevent saving errors
    bq rm -f --model $2:$3.$6
	
	
    bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
    'CREATE MODEL '$3'.'$6'
    OPTIONS(model_type="KMEANS", NUM_CLUSTERS='$8', KMEANS_INIT_METHOD="KMEANS++", MAX_ITERATIONS=50, early_stop=TRUE, MIN_REL_PROGRESS=0.001, WARM_START=FALSE, DISTANCE_TYPE="COSINE") AS 
    SELECT *
    FROM `'$2'.'$3'.'$4'`'
   
   # Evaluate
   evaluate_model $1 $2 $3 $4 $6
 
   # Prédire des nouvelle etiquettes
   predict_with_model $1 $2 $3 $5 $6 $7
     
}


# ---------------------------


# Unsupported dataset location europe-west9 for model BOOSTED_TREE_CLASSIFIER
decision_tree(){

    # Inputs:
    # $1 = location
    # $2 = PROJECT_ID
    # $3 = dataset_name
    # $4 = TRAIN_TABLE_name
    # $5 = TESTING_TABLE_name
    # $6 = MODEL_name
    # $7 = PREDICTED_results_TABLE_name
	
	bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
    'CREATE OR REPLACE MODEL '$3'.'$5'
    OPTIONS(model_type="BOOSTED_TREE_CLASSIFIER", l2_reg = 0.01, num_parallel_tree = 8, max_tree_depth = 10, max_iterations=50, early_stop=TRUE, MIN_REL_PROGRESS=0.001, CATEGORY_ENCODING_METHOD="LABEL_ENCODING") AS 
    SELECT *
    FROM `'$2'.'$3'.'$4'`'

   # Evaluate
   evaluate_model $1 $2 $3 $4 $6
 
   # Prédire des nouvelle etiquettes
   predict_with_model $1 $2 $3 $5 $6 $7
   	
}


# ---------------------------



random_forest(){

    # Inputs:
    # $1 = location
    # $2 = PROJECT_ID
    # $3 = dataset_name
    # $4 = TRAIN_TABLE_name
    # $5 = TESTING_TABLE_name
    # $6 = MODEL_name
    # $7 = PREDICTED_results_TABLE_name
	
	
	bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
    'CREATE OR REPLACE MODEL '$3'.'$5'
    OPTIONS(model_type="RANDOM_FOREST_CLASSIFIER", l2_reg = 0.01, num_parallel_tree=8, max_tree_depth = 10, max_iterations=50, early_stop=TRUE, MIN_REL_PROGRESS=0.001, INPUT_LABEL_COLS=["label"]) AS 
    SELECT *
    FROM `'$2'.'$3'.'$4'`'

   # Evaluate
   evaluate_model $1 $2 $3 $4 $6
 
   # Prédire des nouvelle etiquettes
   predict_with_model $1 $2 $3 $5 $6 $7
   
}


# ---------------------------



deep_neural_network(){

    # Inputs:
    # $1 = location
    # $2 = PROJECT_ID
    # $3 = dataset_name
    # $4 = TRAIN_TABLE_name
    # $5 = TESTING_TABLE_name
    # $6 = MODEL_name
    # $7 = PREDICTED_results_TABLE_name
	
     # Invalid OPTIMIZER 'adam'. Valid optimizer with L1/L2 reguralization specified are Adagrad, Ftrl and SGD.

	bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
    'CREATE MODEL '$2'.'$3'.'$5'
    OPTIONS(MODEL_TYPE="DNN_CLASSIFIER", INPUT_LABEL_COLS = ["label"]) 
    AS SELECT *
    FROM `'$2'.'$3'.'$4'`'

   # Evaluate
   evaluate_model $1 $2 $3 $4 $6
 
   # Prédire des nouvelle etiquettes
   predict_with_model $1 $2 $3 $5 $6 $7
   
}


# ---------------------------



autoML_model(){

    # Inputs:
    # $1 = location
    # $2 = PROJECT_ID
    # $3 = dataset_name
    # $4 = TRAIN_TABLE_name
    # $5 = TESTING_TABLE_name
    # $6 = MODEL_name
    # $7 = PREDICTED_results_TABLE_name
	

	bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
    'CREATE MODEL '$2'.'$3'.'$5'
    OPTIONS(MODEL_TYPE="AUTOML_CLASSIFIER", INPUT_LABEL_COLS = ["label"], OPTIMIZATION_OBJECTIVE="MAXIMIZE_AU_ROC") 
    AS SELECT *
    FROM `'$2'.'$3'.'$4'`'

   # Evaluate
   evaluate_model $1 $2 $3 $4 $6
 
   # Prédire des nouvelle etiquettes
   predict_with_model $1 $2 $3 $5 $6 $7
   
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


# ---------------------------------------------


delete_tables_using_a_list(){

	# Inputs:
	# $1 = PROJECT_ID
	# $2 = dataset_name
	
	
	
    echo "---------------- Query Delete Tables ----------------"
    
    cd /home/oem2/Documents/ONLINE_CLASSES/Spécialisation_Google_Data_Analytics/3_Google_Data_Analytics_Capstone_Complete_a_Case_Study/ingestion_folder_bikeshare/csvdata/similar_match_header/exact_match_header
    
    # cd /home/oem2/Documents/ONLINE_CLASSES/Spécialisation_Google_Data_Analytics/3_Google_Data_Analytics_Capstone_Complete_a_Case_Study/ingestion_folder_bikeshare/csvdata/exact_match_header
    
    # put table names to be deleted in a file list
    # bq ls --format=json $PROJECT_ID:$dataset_name | jq -r .[].tableReference.tableId >> table_list_names
    
    for TABLE_name in $(cat table_list_names)
    do
       # -t signifies table
       bq rm -t $1:$2.$TABLE_name
    done
    
 
}


# ---------------------------------------------



# ---------------------------
# Functions END
# ---------------------------





# ---------------------------------------------


download_data(){

		
	# ---------------------------------------------
	# Set Desired location
	# https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html
	# ---------------------------------------------
	# Set the project region/location
	export region=$(echo "eu-west-3")

	# Set desired output format
	export output=$(echo "text")  # json, yaml, yaml-stream, text, table

	# ---------------------------------------------



	# ---------------------------------------------
	# Setup ROOT AWS credentials 
	# ---------------------------------------------
	export val=$(echo "X0")

	if [[ $val == "X0" ]]
	then 
	    # Generate a new ROOT access key : This information gets sent to the /home/oem2/.aws/credentials and /home/oem2/.aws/config files
	    # AWS site: Go to https://aws.amazon.com/fr/ 
	    # 	- Services - Security, Identity, & Compliance - IAM
	    # 	- To go to root user settings : Quick Links - My security credentials
	    # 	- Open CloudShell - aws iam create-access-key
	    # 
	    #}   "CreateDate": "2023-06-21T09:20:32+00:00"ZY2s6SVxna6uvGfCCDK",
	    #{
	    #"AccessKey": {
	    #    "AccessKeyId": "AKIAUZDPRVKIJFUOJANI",
	    #    "Status": "Active",
	    #    "SecretAccessKey": "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",
	    #    "CreateDate": "2023-06-21T09:20:32+00:00"
	    #}
	    #
	    # OR
	    # 
	    # PC terminal (did not work) - aws iam create-access-key
	    
	    # Set configuration file : aws_access_key_id and aws_secret_access_key are automatically put in /home/oem2/.aws/credentials and /home/oem2/.aws/config files
	    aws configure set region $region
	    aws configure set output $output
	    
	    export AWS_ACCESS_KEY_ID=$(echo "AKIAUZDPRVKIJFUOJANI")
	    export AWS_SECRET_ACCESS_KEY=$(echo "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")
	else
	    echo "Do not setup ROOT AWS credentials"
	fi



	# ---------------------------------------------
	# Setup USER AWS credentials 
	# ---------------------------------------------
	export val=$(echo "X1")

	if [[ $val == "X0" ]]
	then 
	    # Follow instructions at CREATE a username
	    
	    export USERNAME=$(echo "jamilah")
	    
	    aws iam create-access-key --user-name $USERNAME
	    
	    # Set configuration file: put information into /home/oem2/.aws/credentials and /home/oem2/.aws/config files
	    aws configure set aws_access_key_id AKIAUZDPRVKIPHOVBEXG --profile $USERNAME
	    aws configure set aws_secret_access_key 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 --profile $USERNAME
	    aws configure set region $region --profile $USERNAME
	    aws configure set output $output --profile $USERNAME
	    
	    # Set environmental variables
	    export AWS_ACCESS_KEY_ID=$(echo "AKIAUZDPRVKIPHOVBEXG")
	    export AWS_SECRET_ACCESS_KEY=$(echo "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")
	else
	    echo "Do not setup USER AWS credentials"
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
	# Storage S3 commands
	# ---------------------------------------------
	export val=$(echo "X0")

	if [[ $val == "X0" ]]
	then 
	    # List buckets and objects
	    aws s3 ls
	    
	    # Download files from a public S3 Bucket
	    export bucket_name=$(echo "divvy-tripdata")  # https://divvy-tripdata.s3.amazonaws.com/index.html
	    
	    export cur_path=$(pwd)
	    echo "cur_path"
	    echo $cur_path

	    export folder_2_organize=$(echo "bike_casestudy/dataORG")
	    echo "folder_2_organize"
	    echo $folder_2_organize

	    export path_folder_2_organize=$(echo "${cur_path}/${folder_2_organize}")

	    aws s3 cp --recursive s3://$bucket_name $path_folder_2_organize
	    
	else
	    echo ""
	fi

	# ---------------------------------------------


}




# ---------------------------------------------


UNION_table_type0(){

    # Inputs:
    # $1 = location
    # $2 = PROJECT_ID
    # $3 = dataset_name
    # $3 = cur_path
    
    
    
    echo "---------------- Query UNION the Tables ----------------"
    # Key : All the headers for the files need to be the same
    
    
    cd $cur_path
    
    export output_TABLE_name_prev=$(echo "output_TABLE_name_prev")
    export output_TABLE_name_cur=$(echo "output_TABLE_name_cur")
    
    # bq ls $PROJECT_ID:$dataset_name
    cnt=0
    for TABLE_name in $(cat table_list_names)
    do  
        echo "TABLE_name:"
        echo $TABLE_name
        
        
        # The output_TABLE_name can not be the same as an existing table, so it is necessary to change the table name after a UNION 
        if [[ $cnt == 0 ]]; then
            # Just save this table to have and output name
            
            # Create output_TABLE_name_prev
            echo "-----------------------------------"
            bq query \
            --location=$location \
            --destination_table $PROJECT_ID:$dataset_name.$output_TABLE_name_prev \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT CAST(ride_id AS STRING) AS ride_id,
            CAST(rideable_type AS STRING) AS rideable_type,
            CAST(started_at AS STRING) AS started_at,
            CAST(ended_at AS STRING) AS ended_at,
             CAST(start_station_name AS STRING) AS start_station_name,
             CAST(start_station_id AS STRING) AS start_station_id,
             CAST(end_station_name AS STRING) AS end_station_name,
             CAST(end_station_id AS STRING) AS end_station_id,
             CAST(start_lat AS STRING) AS start_lat,
             CAST(start_lng AS STRING) AS start_lng,
             CAST(end_lat AS STRING) AS end_lat,
             CAST(end_lng AS STRING) AS end_lng,
             CAST(member_casual AS STRING) AS member_casual
             FROM `northern-eon-377721.google_analytics.'$TABLE_name'`;'
            echo "-----------------------------------"
            
            # It fails: says table is not found ... does it take time to make the table??
            # echo "-----------------------------------"
            # echo "Print length of Unioned table"
            # Print length of Unioned table
            # bq query \
            # --location=$location \
            # --allow_large_results \
            # --use_legacy_sql=false \
            # 'SELECT COUNT(*) FROM `northern-eon-377721.google_analytics.'$output_TABLE_name_cur'`;'
            echo "-----------------------------------"
            
        else
            
            # Here the table exists and it puts start_station_id and end_station_id as INTEGER
            echo "-----------------------------------"
            echo "Confirm matching table schema of both tables:"
            echo "-----------------------------------"
            echo "Show schema of output_TABLE_name_prev"
            bq --location=$location show \
		--schema \
		--format=prettyjson \
		$PROJECT_ID:$dataset_name.$output_TABLE_name_prev
            
            echo "Show schema of TABLE_name"
            bq --location=$location show \
		--schema \
		--format=prettyjson \
		$PROJECT_ID:$dataset_name.$TABLE_name
            echo "-----------------------------------"
            
            
            echo "-----------------------------------"
            echo "Union tables"
            # Union tables : UNION only takes distinct values, but UNION ALL keeps all of the values selected
            bq query \
            --location=$location \
            --destination_table $PROJECT_ID:$dataset_name.$output_TABLE_name_cur \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT CAST(ride_id AS STRING) AS ride_id,
            CAST(rideable_type AS STRING) AS rideable_type,
            CAST(started_at AS STRING) AS started_at,
            CAST(ended_at AS STRING) AS ended_at,
            CAST(start_station_name AS STRING) AS start_station_name,
            CAST(start_station_id AS STRING) AS start_station_id,
            CAST(end_station_name AS STRING) AS end_station_name,
            CAST(end_station_id AS STRING) AS end_station_id,
            CAST(start_lat AS STRING) AS start_lat,
            CAST(start_lng AS STRING) AS start_lng,
            CAST(end_lat AS STRING) AS end_lat,
            CAST(end_lng AS STRING) AS end_lng,
            CAST(member_casual AS STRING) AS member_casual
            FROM `northern-eon-377721.google_analytics.'$output_TABLE_name_prev'`
            UNION ALL 
            SELECT CAST(ride_id AS STRING) AS ride_id,
            CAST(rideable_type AS STRING) AS rideable_type,
            CAST(started_at AS STRING) AS started_at,
            CAST(ended_at AS STRING) AS ended_at,
            CAST(start_station_name AS STRING) AS start_station_name,
            CAST(start_station_id AS STRING) AS start_station_id,
            CAST(end_station_name AS STRING) AS end_station_name,
            CAST(end_station_id AS STRING) AS end_station_id,
            CAST(start_lat AS STRING) AS start_lat,
            CAST(start_lng AS STRING) AS start_lng,
            CAST(end_lat AS STRING) AS end_lat,
            CAST(end_lng AS STRING) AS end_lng,
            CAST(member_casual AS STRING) AS member_casual
            FROM `northern-eon-377721.google_analytics.'$TABLE_name'`;'
            echo "-----------------------------------"
            
            echo "-----------------------------------"
            echo "Print length of Unioned table"
            # Print length of Unioned table
            bq query \
            --location=$location \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT COUNT(*) FROM `northern-eon-377721.google_analytics.'$output_TABLE_name_cur'`;'
            echo "-----------------------------------"
            
            echo "-----------------------------------"
            #echo "Delete output_TABLE_name_prev"
            # Delete output_TABLE_name_prev because it is now in output_TABLE_name_cur
            bq --location=$location rm -f -t $PROJECT_ID:$dataset_name.$output_TABLE_name_prev
            echo "-----------------------------------"
            
            echo "-----------------------------------"
            # https://cloud.google.com/bigquery/docs/managing-tables#renaming-table
            echo "copy table output_TABLE_name_cur to output_TABLE_name_prev"
            bq --location=$location cp \
            -a -f -n \
            $PROJECT_ID:$dataset_name.$output_TABLE_name_cur \
            $PROJECT_ID:$dataset_name.$output_TABLE_name_prev
            echo "-----------------------------------"
            
            echo "-----------------------------------"
            # Delete output_TABLE_name_cur because it is now in output_TABLE_name_prev
            bq --location=$location rm -f -t $PROJECT_ID:$dataset_name.$output_TABLE_name_cur
            echo "-----------------------------------"
            
        fi
        
        cnt=$((cnt + 1))
        
    done


}


# ---------------------------------------------


UNION_table_type1(){

    # Inputs:
    # $1 = location
    # $2 = PROJECT_ID
    # $3 = dataset_name
    # $3 = cur_path

    echo "---------------- Query UNION the Tables ----------------"
    # Key : All the headers for the files need to be the same
    
    
    cd $cur_path
    
    export output_TABLE_name_prev=$(echo "output_TABLE_name_prev")
    export output_TABLE_name_cur=$(echo "output_TABLE_name_cur")
    
    # bq ls $PROJECT_ID:$dataset_name
    cnt=0
    for TABLE_name in $(cat table_list_names)
    do  
        echo "TABLE_name:"
        echo $TABLE_name
        
        
        # The output_TABLE_name can not be the same as an existing table, so it is necessary to change the table name after a UNION 
        if [[ $cnt == 0 ]]; then
            # Just save this table to have and output name
            
            # Create output_TABLE_name_prev
            echo "-----------------------------------"
            bq query \
            --location=$location \
            --destination_table $PROJECT_ID:$dataset_name.$output_TABLE_name_prev \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT CAST(trip_id AS STRING) AS trip_id,
            CAST(starttime AS STRING) AS starttime,
            CAST(stoptime AS STRING) AS stoptime,
            CAST(bikeid AS STRING) AS bikeid,
             CAST(tripduration AS STRING) AS tripduration,
             CAST(start_station_id AS STRING) AS start_station_id,
             CAST(start_station_name AS STRING) AS start_station_name,
             CAST(end_station_id AS STRING) AS end_station_id,
             CAST(end_station_name AS STRING) AS end_station_name,
             CAST(usertype AS STRING) AS usertype,
             CAST(gender AS STRING) AS gender,
             CAST(birthyear AS STRING) AS birthyear
             FROM `northern-eon-377721.google_analytics.'$TABLE_name'`;'
            echo "-----------------------------------"
            
            # It fails: says table is not found ... does it take time to make the table??
            # echo "-----------------------------------"
            # echo "Print length of Unioned table"
            # Print length of Unioned table
            # bq query \
            # --location=$location \
            # --allow_large_results \
            # --use_legacy_sql=false \
            # 'SELECT COUNT(*) FROM `northern-eon-377721.google_analytics.'$output_TABLE_name_cur'`;'
            echo "-----------------------------------"
            
        else
            
            # Here the table exists and it puts start_station_id and end_station_id as INTEGER
            echo "-----------------------------------"
            echo "Confirm matching table schema of both tables:"
            echo "-----------------------------------"
            echo "Show schema of output_TABLE_name_prev"
            bq --location=$location show \
		--schema \
		--format=prettyjson \
		$PROJECT_ID:$dataset_name.$output_TABLE_name_prev
            
            echo "Show schema of TABLE_name"
            bq --location=$location show \
		--schema \
		--format=prettyjson \
		$PROJECT_ID:$dataset_name.$TABLE_name
            echo "-----------------------------------"
            
            
            echo "-----------------------------------"
            echo "Union tables"
            # Union tables : UNION only takes distinct values, but UNION ALL keeps all of the values selected
            bq query \
            --location=$location \
            --destination_table $PROJECT_ID:$dataset_name.$output_TABLE_name_cur \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT CAST(trip_id AS STRING) AS trip_id,
            CAST(starttime AS STRING) AS starttime,
            CAST(stoptime AS STRING) AS stoptime,
            CAST(bikeid AS STRING) AS bikeid,
             CAST(tripduration AS STRING) AS tripduration,
             CAST(start_station_id AS STRING) AS start_station_id,
             CAST(start_station_name AS STRING) AS start_station_name,
             CAST(end_station_id AS STRING) AS end_station_id,
             CAST(end_station_name AS STRING) AS end_station_name,
             CAST(usertype AS STRING) AS usertype,
             CAST(gender AS STRING) AS gender,
             CAST(birthyear AS STRING) AS birthyear
            FROM `northern-eon-377721.google_analytics.'$output_TABLE_name_prev'`
            UNION ALL 
            SELECT CAST(trip_id AS STRING) AS trip_id,
            CAST(starttime AS STRING) AS starttime,
            CAST(stoptime AS STRING) AS stoptime,
            CAST(bikeid AS STRING) AS bikeid,
             CAST(tripduration AS STRING) AS tripduration,
             CAST(start_station_id AS STRING) AS start_station_id,
             CAST(start_station_name AS STRING) AS start_station_name,
             CAST(end_station_id AS STRING) AS end_station_id,
             CAST(end_station_name AS STRING) AS end_station_name,
             CAST(usertype AS STRING) AS usertype,
             CAST(gender AS STRING) AS gender,
             CAST(birthyear AS STRING) AS birthyear
            FROM `northern-eon-377721.google_analytics.'$TABLE_name'`;'
            echo "-----------------------------------"
            
            echo "-----------------------------------"
            echo "Print length of Unioned table"
            # Print length of Unioned table
            bq query \
            --location=$location \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT COUNT(*) FROM `northern-eon-377721.google_analytics.'$output_TABLE_name_cur'`;'
            echo "-----------------------------------"
            
            echo "-----------------------------------"
            #echo "Delete output_TABLE_name_prev"
            # Delete output_TABLE_name_prev because it is now in output_TABLE_name_cur
            bq --location=$location rm -f -t $PROJECT_ID:$dataset_name.$output_TABLE_name_prev
            echo "-----------------------------------"
            
            echo "-----------------------------------"
            # https://cloud.google.com/bigquery/docs/managing-tables#renaming-table
            echo "copy table output_TABLE_name_cur to output_TABLE_name_prev"
            bq --location=$location cp \
            -a -f -n \
            $PROJECT_ID:$dataset_name.$output_TABLE_name_cur \
            $PROJECT_ID:$dataset_name.$output_TABLE_name_prev
            echo "-----------------------------------"
            
            echo "-----------------------------------"
            # Delete output_TABLE_name_cur because it is now in output_TABLE_name_prev
            bq --location=$location rm -f -t $PROJECT_ID:$dataset_name.$output_TABLE_name_cur
            echo "-----------------------------------"
            
        fi
        
        cnt=$((cnt + 1))
        
    done

}


# ---------------------------------------------


join_two_tables(){

    # Inputs:
    # $1 = location
    # $2 = PROJECT_ID
    # $3 = dataset_name

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


# ---------------------------------------------


CLEAN_TABLE_bikeshare_full_clean0(){

    # Inputs:
    # $1 = location
    # $2 = PROJECT_ID
    # $3 = dataset_name
    
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


# ---------------------------------------------

CLEAN_TABLE_bikeshare_full_clean1(){

    # Inputs:
    # $1 = location
    # $2 = PROJECT_ID
    # $3 = dataset_name
    
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

# ---------------------------------------------

join_multiple_tables(){
	
    # Inputs:
    # $1 = location
    # $2 = PROJECT_ID
    # $3 = dataset_name
    
	# dailyActivity_merged.csv T0
	# [Id, ActivityDate, TotalSteps, TotalDistance, TrackerDistance, LoggedActivitiesDistance, VeryActiveDistance, ModeratelyActiveDistance, LightActiveDistance, SedentaryActiveDistance, VeryActiveMinutes, FairlyActiveMinutes, LightlyActiveMinutes, SedentaryMinutes, Calories]
	
	# minuteCaloriesWide_merged.csv T1
#Id,ActivityHour,Calories00,Calories01, Calories02, Calories03, Calories04, Calories05, Calories06, Calories07, Calories08, Calories09, Calories10, Calories11, Calories12, Calories13, Calories14, Calories15, Calories16, Calories17, Calories18, Calories19, Calories20, Calories21, Calories22, Calories23, Calories24, Calories25, Calories26, Calories27, Calories28, Calories29, Calories30, Calories31, Calories32, Calories33, Calories34, Calories35, Calories36, Calories37, Calories38, Calories39,Calories40, Calories41, Calories42, Calories43, Calories44, Calories45, Calories46, Calories47, Calories48, Calories49, Calories50, Calories51, Calories52, Calories53, Calories54, Calories55, alories56, Calories57, Calories58, Calories59]
     
	# dailyCalories_merged.csv T2
	# [Id, ActivityDay, Calories]
	
	# minuteIntensitiesNarrow_merged.csv T3
	# [Id, ActivityMinute, Intensity
	
	# dailyIntensities_merged.csv T4
	# [Id, ActivityDay, SedentaryMinutes, LightlyActiveMinutes, FairlyActiveMinutes, VeryActiveMinutes, SedentaryActiveDistance, LightActiveDistance, ModeratelyActiveDistance, VeryActiveDistance]

	# minuteIntensitiesWide_merged.csv T5
	# Id, ActivityHour, Intensity00, Intensity01, Intensity02, Intensity03, Intensity04, Intensity05, Intensity06, Intensity07,Intensity08,Intensity09,Intensity10,Intensity11,Intensity12,Intensity13,Intensity14,Intensity15,Intensity16,Intensity17,Intensity18,Intensity19,Intensity20,Intensity21,Intensity22,Intensity23,Intensity24,Intensity25,Intensity26,Intensity27,Intensity28,Intensity29,Intensity30,Intensity31,Intensity32,Intensity33,Intensity34,Intensity35,Intensity36,Intensity37,Intensity38,Intensity39,Intensity40,Intensity41,Intensity42,Intensity43,Intensity44,Intensity45,Intensity46,Intensity47,Intensity48,Intensity49,Intensity50,Intensity51,Intensity52,Intensity53,Intensity54,Intensity55,Intensity56,Intensity57,Intensity58,Intensity59

	# dailySteps_merged.csv T6
	# Id, ActivityDay, StepTotal
	
	# minuteMETsNarrow_merged.csv T7
	# [Id, ActivityMinute, METs]
	
	# heartrate_seconds_merged.csv T8
	# [Id, Time, Value]
	
	# *** Failed to join with dailyActivity_merged
	# minuteSleep_merged.csv T9
	# [Id, date, value, logId]
	
	# hourlyCalories_merged.csv T10
	# [Id, ActivityHour, Calories]
	
	# minuteStepsNarrow_merged.csv T11
	# [Id, ActivityMinutes, Steps]
	
	# hourlyIntensities_merged.csv T12
	# [Id, ActivityHour, TotalIntensity, AverageIntensity]
	
	# minuteStepsWide_merged.csv T13
	# [Id, ActivityHour, Steps00 to Steps59]
	
	# hourlySteps_merged.csv T14
	# Id, ActivityHour, StepTotal]
	
	# *** Failed to join with dailyActivity_merged
	# sleepDay_merged.csv T15
	# Id, SleepDay, TotalSleepRecords, TotalMinutesAsleep, TotalTimeInBed]
	
	# minuteCaloriesNarrow_merged.csv T16 
	# Id, ActivityMinute, Calories]
	
	# weightLogInfo_merged.csv T17
	# [Id, Date, WeightKg, WeightPounds, Fat, BMI, IsManualReport, LogId]
	
	# export x=$(echo "weightLogInfo_merged")
	# VIEW_the_columns_of_a_table $location $PROJECT_ID $dataset_name $x
     
	# T1.ActivityHour AS hour_calories,
	# T2.ActivityDay AS day_calories,
        # T3.ActivityMinute min_intensity, 
        # T3.Intensity,
        # T7.METs,
        
     #INNER JOIN `'$2'.'$3'.minuteCaloriesWide_merged` AS T1 ON T0.Id = T1.Id
     #INNER JOIN `'$2'.'$3'.dailyCalories_merged` AS T2 ON T0.Id = T2.Id
     #INNER JOIN `'$2'.'$3'.minuteIntensitiesNarrow_merged` AS T3 ON T0.Id = T3.Id
     #INNER JOIN `'$2'.'$3'.minuteMETsNarrow_merged` AS T7 ON T0.Id = T7.Id
     
     bq rm -t $2:$3.exercise_full
     
     export TABLE_name_join=$(echo "exercise_full")

     bq query \
            --location=$1 \
            --destination_table $2:$3.$TABLE_name_join \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT 
            T0.ActivityDate, 
            T0.TotalSteps, 
            T0.TotalDistance, 
            T0.VeryActiveDistance,
            T0.ModeratelyActiveDistance, 
            T0.LightActiveDistance, 
            T0.SedentaryActiveDistance, 
            T0.VeryActiveMinutes,
            T0.FairlyActiveMinutes,
            T0.LightlyActiveMinutes,
            T0.SedentaryMinutes,
            T0.Calories,
            T8.Value AS heartrate_time,
            T8.Value AS heartrate,
            T15.TotalTimeInBed AS sleep_duration, 
            T17.WeightKg,
            T17.WeightPounds,
            T17.Fat,
            T17.BMI
            FROM `'$2'.'$3'.dailyActivity_merged` AS T0
	 JOIN `'$2'.'$3'.heartrate_seconds_merged` AS T8 ON T0.Id = T8.Id
	 JOIN `'$2'.'$3'.sleepDay_merged` AS T15 ON T0.Id = T15.Id
	 JOIN `'$2'.'$3'.weightLogInfo_merged` AS T17 ON T0.Id = T17.Id;'   

}

# When you create a query by using a JOIN, consider the order in which you are merging the data. The GoogleSQL query optimizer can determine which table should be on which side of the join, but it is still recommended to order your joined tables appropriately. As a best practice, place the table with the largest number of rows first, followed by the table with the fewest rows, and then place the remaining tables by decreasing size.

# When you have a large table as the left side of the JOIN and a small one on the right side of the JOIN, a broadcast join is created. A broadcast join sends all the data in the smaller table to each slot that processes the larger table. It is advisable to perform the broadcast join first.


# ---------------------------------------------

# WORKED
join_2_tables(){
	
    # Inputs:
    # $1 = location
    # $2 = PROJECT_ID
    # $3 = dataset_name
    # $4 = OUTPUT_TABLE_name

     bq query \
            --location=$1 \
            --destination_table $2:$3.$4 \
            --allow_large_results \
            --use_legacy_sql=false \
            'SELECT 
            T0.Id,
            T0.ActivityDate, 
            T0.TotalSteps, 
            T0.TotalDistance, 
            T0.VeryActiveDistance,
            T0.ModeratelyActiveDistance, 
            T0.LightActiveDistance, 
            T0.SedentaryActiveDistance, 
            T0.VeryActiveMinutes,
            T0.FairlyActiveMinutes,
            T0.LightlyActiveMinutes,
            T0.SedentaryMinutes,
            T0.Calories,
            T8.Value AS heartrate_time,
            T8.Value AS heartrate,
            T15.TotalTimeInBed AS sleep_duration
            FROM `'$2'.'$3'.dailyActivity_merged` AS T0
INNER JOIN `'$2'.'$3'.heartrate_seconds_merged` AS T8 ON T0.Id = T8.Id;'   

}

# ---------------------------------------------




    
