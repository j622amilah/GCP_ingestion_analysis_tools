#!/bin/bash

# ./step0_download_data.sh


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




# ---------------------------------------------
# Setup USER AWS credentials 
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




