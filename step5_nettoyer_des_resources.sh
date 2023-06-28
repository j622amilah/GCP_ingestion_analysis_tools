#!/bin/bash


# ---------------------------------------------
# Delete ROOT AWS credentials 
# ---------------------------------------------
export val=$(echo "X0")

if [[ $val == "X0" ]]
then 
    export AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id)
    export AWS_ACCESS_KEY_ID=$(aws configure get aws_secret_access_key)
    # OR
    export AWS_SECRET_ACCESS_KEY=$(aws configure get default.aws_access_key_id)
    export AWS_SECRET_ACCESS_KEY=$(aws configure get default.aws_secret_access_key)
    
    aws iam delete-access-key --access-key-id $AWS_ACCESS_KEY_ID
else
    echo "Do not setup ROOT AWS credentials"
fi



# ---------------------------------------------
# Delete USER AWS credentials 
# ---------------------------------------------
export val=$(echo "X0")

if [[ $val == "X0" ]]
then 
    # Generate a new ROOT access key : This information gets sent to the /home/oem2/.aws/credentials and /home/oem2/.aws/config files
    export USERNAME=$(echo "jamilah")
    
    export AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id --profile $USERNAME)
    export AWS_ACCESS_KEY_ID=$(aaws configure get aws_secret_access_key --profile $USERNAME)
    # OR
    export AWS_SECRET_ACCESS_KEY=$(aws configure get profile.$USERNAME.aws_access_key_id)
    export AWS_SECRET_ACCESS_KEY=$(aws configure get profile.$USERNAME.aws_secret_access_key)
    
    aws iam delete-access-key --access-key-id $AWS_ACCESS_KEY_ID --user-name $USERNAME
else
    echo "Do not setup USER AWS credentials"
fi
