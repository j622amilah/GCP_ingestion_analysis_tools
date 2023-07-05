#!/bin/bash


# ---------------------------
# Functions START
# ---------------------------

organize_zip_files_from_datasource_download(){
	
		
	# Automatically organizes a folder of zipped csv files into 3 folders: csvdata, remaining_files, zipdata
	
	
	# Inputs:
	# $1 = path_folder_2_organize (path where the zip files are located)
	# $2 = ingestion_folder (folder name to put the files into)
	# $3 = path_outside_of_ingestion_folder (path where the ingestion folder should be located)
	
	# Path inside of the ingestion folder
	export path_ingestion_folder=$(echo "$3/$2")
	
	# ---------------------------------------------
	# Make ingestion folder and transfer files
	# ---------------------------------------------
	export val=$(echo "X0")

	if [[ $val == "X0" ]]
	then

	    mkdir $path_ingestion_folder
	    cp -a $1/. $path_ingestion_folder
		
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
	    
	    # Rename all the files so they do not have any spaces
	    for f in *\ *; do mv "$f" "${f// /_}"; done
	    
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
       # Remove AM and PM from all csv files to prevent TIMESTAMP error : *** could be better ***
       cat $CSV_NAME | sed 's/ AM//g' | sed 's/ PM//g' > temp_csv
       mv temp_csv $CSV_NAME
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
	
    # Bigquery needs numerical features and labels
	
    # Ensure that the OUTPUT model does not already exist, to prevent saving errors
    bq rm -f --model $2:$3.$6
	
	
    bq query \
            --location=$1 \
            --allow_large_results \
            --use_legacy_sql=false \
    'CREATE MODEL '$3'.'$6'
    OPTIONS(model_type="KMEANS", NUM_CLUSTERS=2, KMEANS_INIT_METHOD="KMEANS++", MAX_ITERATIONS=50, early_stop=TRUE, MIN_REL_PROGRESS=0.001, WARM_START=FALSE, DISTANCE_TYPE="COSINE") AS 
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



# -----------------
# Backup/Copy table
# -----------------
export val=$(echo "X1")

if [[ $val == "X0" ]]
then 
    
    echo "Backup/Copy table:"
    export output_TABLE_name=$(echo "output_TABLE_name_prev")
    # export output_TABLE_name_backup=$(echo "bikeshare_table0")
    export output_TABLE_name_backup=$(echo "bikeshare_table1")
    
    bq --location=$location cp \
            -a -f -n \
            $PROJECT_ID:$dataset_name.$output_TABLE_name \
            $PROJECT_ID:$dataset_name.$output_TABLE_name_backup

fi


# ---------------------------------------------




# ---------------------------------------------


# -----------------
# List buckets
# -----------------
export val=$(echo "X1")

if [[ $val == "X0" ]]
then 
    
    echo "list buckets"
    gcloud storage ls

fi


# ---------------------------------------------


export val=$(echo "X1")

if [[ $val == "X0" ]]
then 
    
    export output_TABLE_name_prev=$(echo "output_TABLE_name_prev")
    
    # Save final_table in bucket
    bq extract --location=$location \
               --destination_format=CSV \
               --field_delimiter=',' \
               --print_header=boolean \
               $PROJECT_ID:$dataset_name.$output_TABLE_name_prev \
               gs://the_bucket4/myFile.csv
    
fi


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

export val=$(echo "X1")

if [[ $val == "X0" ]]
then 
    echo "---------------- update the table schema ----------------"
    
    cd $cur_path
    
    cnt=0
    for TABLE_name in $(cat table_list_names)
    do  
        echo "TABLE_name:"
        echo $TABLE_name
    
	bq --location=$location update $PROJECT_ID:$dataset_name.$TABLE_name ${cur_path}/myschema.json
    
        cnt=$((cnt + 1))
        
    done
    
fi

# ---------------------------------------------


# ---------------------------------------------


# ---------------------------
# Functions END
# ---------------------------



