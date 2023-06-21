#!/bin/bash

# ./step1_open_files.sh



export cur_path=$(pwd)
echo "cur_path"
echo $cur_path

export folder_2_organize=$(echo "bike_casestudy/dataORG")
echo "folder_2_organize"
echo $folder_2_organize

export path_folder_2_organize=$(echo "${cur_path}/${folder_2_organize}")


export ingestion_folder=$(echo "ingestion_folder")
echo "ingestion_folder"
echo $ingestion_folder

export path_ingestion_folder=$(echo "${cur_path}/${ingestion_folder}")




# ---------------------------------------------
# Make ingestion folder and transfer files
# ---------------------------------------------
export val=$(echo "X0")

if [[ $val == "X0" ]]
then

    mkdir $path_ingestion_folder

    cp -a $path_folder_2_organize/. $path_ingestion_folder
	
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




