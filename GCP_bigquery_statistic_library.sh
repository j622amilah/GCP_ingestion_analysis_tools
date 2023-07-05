#!/bin/bash

# cd /home/oem2/Documents/ONLINE_CLASSES/Spécialisation_Google_Data_Analytics/3_Google_Data_Analytics_Capstone_Complete_a_Case_Study/git2/automatic_GCP_ingestion

# source ./GCP_bigquery_statistic_library.sh



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


# Calculate the p-value per z-statistic or t-statistic [To do]


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


# TYPE A RESULTS : probability of a categorical event happening 

# Additive rule of probability: P(A or B) = P(A) + P(B) - P(A and B)
# ie: the probablity of a ([casual user being female] OR [casual user being male]) AND [casual user using an electric_bike]
# (Reponse)  (0.004766284784788248 + 0.007837291891818123) * 0.08795399798808465

# Multiplicative rule of probability: P(A and B) = P(A) * P(B)
# ie: the probablity of a [casual user being female] AND [casual user using an electric_bike]
# (Reponse)  0.004766284784788248 * 0.08795399798808465


# Statistical significance of probablistic count for CATEGORICAL features
# *** NOT AUTOMATED, but written out *** 


# export TABLE_name=$(echo "bikeshare_full_clean1")
# export TABLE_name_probcount=$(echo "bikeshare_full_clean1_CATprobcount")

# Calculation of percentage/probability of occurence across all samples
# bq query \
#     --location=$location \
#     --destination_table $PROJECT_ID:$dataset_name.$TABLE_name_probcount \
#     --allow_large_results \
#     --use_legacy_sql=false \
# 'SELECT ROW_NUMBER() OVER(ORDER BY member_casual) AS row_num,
# member_casual, 
# rideable_type, 
# gender, 
# COUNT(*)/(SELECT COUNT(*) FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`) AS prob_perc
# FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`
# GROUP BY member_casual, rideable_type, gender
# ORDER BY member_casual, rideable_type, gender;'


# Is the probability occurence (percentage) per group across all samples statistically significant?
# Could improve this and add the p-value function as a new column
# export prob_perc=$(echo "prob_perc")  # name of numerical column to find z-statistic values per row
# ONE_SAMPLE_TESTS_zstatistic_per_row $location $prob_perc $PROJECT_ID $dataset_name $TABLE_name_probcount




# ---------------------------------------------


# Statistical significance of probablistic count for NUMERICAL features per BIN_NUMBER
# *** NOT AUTOMATED, but written out *** 

# export TABLE_name=$(echo "bikeshare_full_clean1")
# export TABLE_name_probcount=$(echo "TABLE_name_probcount")

# Calculation of percentage/probability of occurence of a numerical feature (workout_minutes) for a bin_number [ie: days (weekday=5, weekend=2)] across all samples
# bq query \
#     --location=$location \
#     --destination_table $PROJECT_ID:$dataset_name.$TABLE_name_probcount \
#     --allow_large_results \
#     --use_legacy_sql=false \
# 'WITH tab2 AS
# (
#   SELECT *, 
#   (SELECT SUM(workout_minutes)/bin_number FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'` WHERE wday ="weekend") AS pop_weekend 
#   FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`)
# )
# SELECT lifestyle, wday, (SUM(workout_minutes)/bin_number)/AVG(pop_weekend) AS prob_perc 
# FROM tab2
# GROUP BY lifestyle, wday
# ORDER BY wday, lifestyle;'


# Is the probability occurence (percentage) per group across all samples statistically significant?
# Could improve this and add the p-value function as a new column
# export prob_perc=$(echo "prob_perc")  # name of numerical column to find z-statistic values per row
# ONE_SAMPLE_TESTS_zstatistic_per_row $location $prob_perc $PROJECT_ID $dataset_name $TABLE_name_probcount



# ---------------------------------------------









