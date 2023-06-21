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




