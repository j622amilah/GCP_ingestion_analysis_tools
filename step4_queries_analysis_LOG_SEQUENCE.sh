
# ---------------------------------------------
# Agreed, need to make a new table that is condensed with useful featueres
# ---------------------------------------------


0. combine ride_id and trip_id

SELECT IF(ride_id=NULL, trip_id, IF(trip_id=NULL, ride_id, NULL)) AS fin_trip_ID

bq query \
    --location=$location \
    --allow_large_results \
    --use_legacy_sql=false \
    'SELECT IF(ride_id = trip_id AND ride_id IS NOT NULL, CONCAT(ride_id, ", ", trip_id), COALESCE(ride_id, trip_id)) AS fin_stsname FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`;'


1. good feature : what are the categories??
rideable_type

    bq query \
    --location=$location \
    --allow_large_results \
    --use_legacy_sql=false \
    'SELECT rideable_type FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`
    GROUP BY rideable_type;'

+---------------+
| rideable_type |
+---------------+
| docked_bike   |
| electric_bike |
| classic_bike  |
| NULL          |
+---------------+


2. started_at, starttime

bq query \
    --location=$location \
    --allow_large_results \
    --use_legacy_sql=false \
    'SELECT started_at, starttime FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`
    ORDER BY starttime, started_at DESC
    LIMIT 10;'
    
+------------------------+-----------+
|       started_at       | starttime |
+------------------------+-----------+
| 2023-05-31 23:59:58+00 | NULL      |
| 2023-05-31 23:59:49+00 | NULL      |
| 2023-05-31 23:59:44+00 | NULL      |
| 2023-05-31 23:59:30+00 | NULL      |
| 2023-05-31 23:59:09+00 | NULL      |
| 2023-05-31 23:59:07+00 | NULL      |
| 2023-05-31 23:58:49+00 | NULL      |
| 2023-05-31 23:58:39+00 | NULL      |
| 2023-05-31 23:58:32+00 | NULL      |
| 2023-05-31 23:57:49+00 | NULL      |
+------------------------+-----------+

    bq query \
    --location=$location \
    --allow_large_results \
    --use_legacy_sql=false \
    'SELECT started_at, starttime FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`
    WHERE starttime IS NOT NULL AND started_at IS NOT NULL
    ORDER BY starttime, started_at DESC
    LIMIT 10;'
    
    confirm if starttime is all NULL
    
    bq query \
    --location=$location \
    --allow_large_results \
    --use_legacy_sql=false \
    'SELECT started_at, starttime FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`
    WHERE started_at IS NOT NULL
    ORDER BY starttime, started_at DESC
    LIMIT 10;'
    
    starttime is all NULL  ---> do not even use
    
    
    
3. ended_at, stoptime

    bq query \
    --location=$location \
    --allow_large_results \
    --use_legacy_sql=false \
    'SELECT ended_at, stoptime FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`
    ORDER BY stoptime, ended_at DESC
    LIMIT 10;'

+------------------------+----------+
|        ended_at        | stoptime |
+------------------------+----------+
| 2023-06-07 23:04:26+00 | NULL     |
| 2023-06-07 15:02:09+00 | NULL     |
| 2023-06-07 09:49:45+00 | NULL     |
| 2023-06-06 11:59:30+00 | NULL     |
| 2023-06-06 07:30:59+00 | NULL     |
| 2023-06-06 07:16:34+00 | NULL     |
| 2023-06-06 07:11:00+00 | NULL     |
| 2023-06-06 07:08:19+00 | NULL     |
| 2023-06-06 04:26:02+00 | NULL     |
| 2023-06-05 09:07:59+00 | NULL     |
+------------------------+----------+

    bq query \
    --location=$location \
    --allow_large_results \
    --use_legacy_sql=false \
    'SELECT ended_at, stoptime FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`
    WHERE ended_at IS NOT NULL
    ORDER BY stoptime, ended_at DESC
    LIMIT 10;'



4. stsname_T0, stsname_T1

# Will return the first value that is not NULL
COALESCE(stsname_T0, stsname_T1)

# 
SELECT IF(stsname_T0 = stsname_T1 AND stsname_T0 IS NOT NULL, CONCAT(stsname_T0, ", ", stsname_T1), COALESCE(stsname_T0, stsname_T1)) AS fin_stsname

if stsname_T0 == stsname_T1: 
    # pick stsname_T0
    out = stsname_T0   # could be a value or NULL
else:
    if stsname_T0 == NULL and stsname_T1 != NULL:
         out = stsname_T1
         
    if stsname_T0 != NULL and stsname_T1 == NULL:
         out = stsname_T0
    
    else 
        # both stsname_T0 and stsname_T1 are not equal, nor NULL
	concat()


bq query \
    --location=$location \
    --allow_large_results \
    --use_legacy_sql=false \
    'SELECT IF(stsname_T0 = stsname_T1 AND stsname_T0 IS NOT NULL, CONCAT(stsname_T0, ", ", stsname_T1), COALESCE(stsname_T0, stsname_T1)) AS fin_stsname FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`;'


5. ssid_T0, ssid_T1

bq query \
    --location=$location \
    --allow_large_results \
    --use_legacy_sql=false \
    'SELECT IF(stsname_T0 = stsname_T1 AND stsname_T0 IS NOT NULL, CONCAT(stsname_T0, ", ", stsname_T1), COALESCE(stsname_T0, stsname_T1)) AS fin_stsname FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`;'


6. esname_T0, esname_T1
7. esid_T0, esid_T1
8. start_lat
9. start_lng
10. end_lat
11. end_lng
12. member_casual
13. usertype
14. gender
15. birthyear

# Initial joint query 

bq query \
            --location=$location \
            --destination_table $PROJECT_ID:$dataset_name.$TABLE_name_clean0 \
            --allow_large_results \
            --use_legacy_sql=false \
    'SELECT COALESCE(ride_id, trip_id) AS fin_trip_ID,
    rideable_type,
    IF(started_at = starttime AND started_at IS NOT NULL, CONCAT(started_at, ", ", starttime), COALESCE(started_at, starttime)) AS fin_starttime,
    IF(ended_at = stoptime AND ended_at IS NOT NULL, CONCAT(ended_at, ", ", stoptime), COALESCE(ended_at, stoptime)) AS fin_endtime,
    IF(stsname_T0 = stsname_T1 AND stsname_T0 IS NOT NULL, CONCAT(stsname_T0, ", ", stsname_T1), COALESCE(stsname_T0, stsname_T1)) AS fin_stsname,
    IF(ssid_T0 = ssid_T1 AND ssid_T0 IS NOT NULL, CONCAT(ssid_T0, ", ", ssid_T1), COALESCE(ssid_T0, ssid_T1)) AS fin_stsID,
    IF(esname_T0 = esname_T1 AND esname_T0 IS NOT NULL, CONCAT(esname_T0, ", ", esname_T1), COALESCE(esname_T0, esname_T1)) AS fin_esname,
    IF(esid_T0 = esid_T1 AND esid_T0 IS NOT NULL, CONCAT(esid_T0, ", ", esid_T1), COALESCE(esid_T0, esid_T1)) AS fin_esID,
    start_lat,
    start_lng,
    end_lat,
    end_lng,
    member_casual,
    usertype,
    gender,
    birthyear
     FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`;'
     


# Way 0: Do COALESCE 
# it is joined on ride_id/trip_id so just pick one [ ride_id OR COALESCE(ride_id, trip_id) ]

# I think you need to do COALESCE on one variable like COALESCE(ended_at, stoptime), and then create a integer_selection number (1,2) of which of the two were selected for the other COALESCE variables to be correctly choosen 


# Way 1: Do COALESCE and NULL the values if there are conflicting values - quick
# instead of doing CONCAT of the two data values, make it NULL because if there are two values the data is unrealiable.


bq query \
            --location=$location \
            --destination_table $PROJECT_ID:$dataset_name.$TABLE_name_clean0 \
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
    start_lat,
    start_lng,
    end_lat,
    end_lng,
    member_casual,
    usertype,
    gender,
    birthyear
     FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`;'




# Need to reduce the table further : 0) starttime and endtime into tripduration, 1) convert start_lat and start_lng with end_lat and end_lng into total_distance

# ----------------------
# tripduration
# ----------------------
CAST(DateTime AS TIMESTAMP)
to_timestamp(datetime) as datetime,

# timestamp difference in seconds
IF(CAST(tripduration AS INTEGER) > TIMESTAMP_DIFF(end_TIMESTAMP, start_TIMESTAMP, SECOND), CAST(tripduration AS INTEGER), TIMESTAMP_DIFF(end_TIMESTAMP, start_TIMESTAMP, SECOND)) AS trip_time, 



# ----------------------
# total_distance
# ----------------------
# Error in query string: Error processing job 'northern-eon-377721:bqjob_r72474243827455f6_00000189037b4479_1': Syntax error: Expected keyword JOIN but got "," at [10:62]
(SELECT MAX(made_up_varname) FROM (VALUES (start_lat_NUM),(end_lat_NUM)) AS made_up_NAME(made_up_varname)) AS MAX_LAT,
(SELECT MIN(made_up_varname) FROM (VALUES (start_lat_NUM),(end_lat_NUM)) AS made_up_NAME(made_up_varname)) AS MIN_LAT,
(SELECT MAX(made_up_varname) FROM (VALUES (start_lng_NUM),(end_lng_NUM)) AS made_up_NAME(made_up_varname)) AS MAX_LONG,
(SELECT MIN(made_up_varname) FROM (VALUES (start_lng_NUM),(end_lng_NUM)) AS made_up_NAME(made_up_varname)) AS MIN_LONG,

# OR

# Error in query string: Error processing job 'northern-eon-377721:bqjob_r4f5232a605952910_00000189037c38bf_1': No matching signature for operator > for argument types: STRING, INT64. Supported signature: ANY > ANY at [23:9]
IF(start_lat_NUM > end_lat_NUM, start_lat_NUM, end_lat_NUM) AS MAX_LAT,
IF(start_lat_NUM < end_lat_NUM, start_lat_NUM, end_lat_NUM) AS MIN_LAT,
IF(start_lng_NUM > end_lng_NUM, start_lng_NUM, end_lng_NUM) AS MAX_LONG,
IF(start_lng_NUM < end_lng_NUM, start_lng_NUM, end_lng_NUM) AS MIN_LONG,

# Error in query string: Error processing job 'northern-eon-377721:bqjob_r72a6fe120367ed9a_00000189037d6623_1': No matching signature for operator > for argument types: STRING, INT64. Supported signature: ANY > ANY at [19:9]


CAST(ABS((MAX_LAT - MIN_LAT) + (MAX_LONG - MIN_LONG)) AS NUMERIC (10,4)) AS trip_distance,
     
     
     
+---------------+-----------+
|  column_name  | data_type |
+---------------+-----------+
| fin_trip_ID   | STRING    |
| rideable_type | STRING    |
| trip_time     | INT64     |
| fin_stsname   | STRING    |
| fin_stsID     | STRING    |
| fin_esname    | STRING    |
| fin_esID      | STRING    |
| trip_distance | FLOAT64   |
| member_casual | STRING    |
| bikeid_INT    | INT64     |
| gender        | STRING    |
| birthyear_INT | INT64     |
+---------------+-----------+

# Numerical features
trip_time
trip_distance
birthyear_INT
# Categorical features
member_casual
rideable_type
gender












# ---------------------------------------------

# 2. Question 0: How do annual members and casual riders use Cyclistic bikes diﬀerently?


# Main features
bq query \
            --location=$location \
            --allow_large_results \
            --use_legacy_sql=false \
    'SELECT member_casual, AVG(trip_time) AS avg_trip_time, AVG(trip_distance) AS avg_trip_dist, ROUND(AVG(CAST(birthyear AS INTEGER))) AS avg_bday, rideable_type, usertype, gender FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`
    WHERE member_casual IS NOT NULL
    GROUP BY member_casual, rideable_type, usertype, gender;'
    
member_casual |   avg_trip_time    |    avg_trip_dist     | avg_bday | rideable_type | usertype | gender 

# ---------------------------------------------
# There is a problem with the database for birthday and gender 

# True, it means the JOIN ON ride_id=trip_id DOES NOTHING because TABLE0 (member_casual, rideable_type) does not align with TABLE1 (usertype, gender, birthyear)

# Can JOIN ON start_station_id=start_station_id
# Can JOIN ON end_station_id=end_station_id
# 	- Tried this and the identifiers are not unique enough for the tables to be joined

# ---------------------------------------------

bq query \
            --location=$location \
            --allow_large_results \
            --use_legacy_sql=false \
    'SELECT usertype FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`
    WHERE usertype IS NOT NULL
    GROUP BY usertype;'

+------------+
|  usertype  |
+------------+
| Customer   |
| Subscriber |
| Dependent  |
+------------+

# Result : Table0 and Table1 are independent after join on ride_id=trip_id
# Realized that I can transform usertype into member_casual to have features in table0 correspond to features in table1 

# ---------------------------------------------

# Numerical features
trip_time
trip_distance
birthyear_INT
# Categorical features
rideable_type
gender


bq query \
            --location=$location \
            --allow_large_results \
            --use_legacy_sql=false \
    'SELECT member_casual, AVG(trip_time) AS avg_trip_time, AVG(trip_distance) AS avg_trip_dist, ROUND(AVG(birthyear_INT)) AS avg_bday, rideable_type, gender FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`
    WHERE member_casual IS NOT NULL
    GROUP BY member_casual, rideable_type, gender;'
+---------------+--------------------+-----------------------+----------+---------------+--------+
| member_casual |   avg_trip_time    |     avg_trip_dist     | avg_bday | rideable_type | gender |
+---------------+--------------------+-----------------------+----------+---------------+--------+
| member        |  833.7740277510674 |   0.01387101766204006 |     NULL | classic_bike  | NULL   |
| member        |   794.448360785873 |  7.983468013723733E-4 |     NULL | docked_bike   | NULL   |
| member        |  879.6812867204748 |                  NULL |   1983.0 | NULL          | Female |
| casual        | 2796.5354765765883 |                  NULL |   1987.0 | NULL          | Male   |
| member        | 1035.3587420636393 |                  NULL |   1982.0 | NULL          | NULL   |
| member        |  979.5392781316351 |                  NULL |   1980.0 | NULL          |        |
| casual        |  2365.210185003487 |                  NULL |   1986.0 | NULL          | NULL   |
| casual        | 3981.8819878686563 |  8.907837918894339E-4 |     NULL | docked_bike   | NULL   |
| member        |  740.5064831221684 |                  NULL |   1981.0 | NULL          | Male   |
| casual        | 3193.1957362500175 |                  NULL |   1988.0 | NULL          | Female |
| casual        | 1020.3394345099119 |   0.00455135762513729 |     NULL | electric_bike | NULL   |
| member        |  653.9106417037417 |   9.81826909246977E-4 |     NULL | electric_bike | NULL   |
| casual        | 1735.6100788573422 | 0.0047875075348582504 |     NULL | classic_bike  | NULL   |
| casual        |  1860.214190824524 |                  NULL |   1973.0 | NULL          |        |
+---------------+--------------------+-----------------------+----------+---------------+--------+

# [0] Do individual comparisons of each of the 5 features : compare member vs casual for  avg_trip_time

bq query \
            --location=$location \
            --allow_large_results \
            --use_legacy_sql=false \
    'SELECT member_casual, AVG(trip_time) AS avg_trip_time FROM `'$PROJECT_ID'.'$dataset_name'.'$TABLE_name'`
    WHERE member_casual IS NOT NULL
    GROUP BY member_casual;'




# how to condense other features 
fin_trip_ID
fin_stsname
fin_stsID
fin_esname
fin_esID
bikeid


# ---------------------------------------------
# Statistics 
# https://cloud.google.com/bigquery/docs/reference/standard-sql/statistical_aggregate_functions#stddev
# ---------------------------------------------
# 2. Question 0: How do annual members and casual riders use Cyclistic bikes diﬀerently?


# [ONE SAMPLE TESTS : testing significance with respect to the mean of a numerical feature FOR a categorical feature category (sample population) ]
# Determine if a numerical feature for a category of another feature is statistically different than the mean of the numerical feature regardless of considering the categorical feature

# Does being a member mean that trip_time is significantly lower with respect to the mean of trip_time (regardless of being a member or not)?
	

Categorical feature:
member_casual
Numerical feature:
trip_distance
+---------------+----------------------+----------------------+-----------------------+------------------+
| member_casual | z_critical_onesample | t_critical_onesample |     avg_samp1_VAR     | df_sample_number |
+---------------+----------------------+----------------------+-----------------------+------------------+
| member        |    0.879237652959979 |   0.7805120094229702 |  0.006629366046007349 |         23815154 |
| casual        |  -0.8883783713928745 |  -1.1361523608536124 | 0.0037317409782921546 |         11647954 |
+---------------+----------------------+----------------------+-----------------------+------------------+
Numerical feature:
trip_time
+---------------+----------------------+----------------------+--------------------+------------------+
| member_casual | z_critical_onesample | t_critical_onesample |   avg_samp1_VAR    | df_sample_number |
+---------------+----------------------+----------------------+--------------------+------------------+
| member        |  -124.50965197888024 |  -206.05286801679762 |  769.1156600540992 |         23815154 |
| casual        |   178.03479666544126 |   117.54641282041989 | 2118.0848747342275 |         11647954 |
+---------------+----------------------+----------------------+--------------------+------------------+
Numerical feature:
birthyear_INT
+---------------+----------------------+----------------------+--------------------+------------------+
| member_casual | z_critical_onesample | t_critical_onesample |   avg_samp1_VAR    | df_sample_number |
+---------------+----------------------+----------------------+--------------------+------------------+
| member        |   -75.84176670734924 |   -75.96042235256981 | 1981.1941846065026 |         23815154 |
| casual        |   1892.5334276437748 |    2122.193011730036 | 1987.4188076871396 |         11647954 |
+---------------+----------------------+----------------------+--------------------+------------------+

******* Need to calculate the mean *******



# https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-label-encoder
# SQL z/t-statistic library and p-value calculation

# [ONE SAMPLE TESTS : testing significance with respect to the mean of a numerical feature FOR a categorical feature category (sample population) ]
# Determine if a numerical feature for a category of another feature is statistically different than the mean of the numerical feature regardless of considering the categorical feature

# Does being a member mean that trip_time is significantly lower with respect to the mean of trip_time (regardless of being a member or not)?

# declare -a CAT_FEATS=('rideable_type' 'gender');

# if I want categorical transformed 


Categorical feature:
member_casual
Transform categorical feature into a Numerical feature:
rideable_type
+---------------+----------------------+----------------------+--------------------+------------------+
| member_casual | z_critical_onesample | t_critical_onesample |   avg_samp1_VAR    | df_sample_number |
+---------------+----------------------+----------------------+--------------------+------------------+
| casual        |    211.0045815099089 |    216.2548939041834 | 2.0533580712491686 |          6744303 |
| member        |   -175.6635532907007 |   -173.4659121757601 | 1.9312360568044353 |          9731001 |
+---------------+----------------------+----------------------+--------------------+------------------+
+---------------+------------------+---------+
| rideable_type | transformed_FEAT |   f0_   |
+---------------+------------------+---------+
| electric_bike |                3 | 6340477 |
| classic_bike  |                1 | 6649756 |
| docked_bike   |                2 | 3485071 |
+---------------+------------------+---------+


Categorical feature:
member_casual
Transform categorical feature into a Numerical feature:
gender

+---------------+----------------------+----------------------+--------------------+------------------+
| member_casual | z_critical_onesample | t_critical_onesample |   avg_samp1_VAR    | df_sample_number |
+---------------+----------------------+----------------------+--------------------+------------------+
| casual        |  -179.44903388417762 |  -160.90974788149612 | 1.6218307781127719 |           389725 |
| member        |   29.879808679025018 |   30.016864030609934 | 1.7502884201745363 |         14056749 |
+---------------+----------------------+----------------------+--------------------+------------------+
+--------+------------------+----------+
| gender | transformed_FEAT |   f0_    |
+--------+------------------+----------+
| Male   |                2 | 10788959 |
| Female |                1 |  3657515 |
+--------+------------------+----------+

******* Need to calculate the mean *******






# [TWO_SAMPLE_TESTS (z-statistic ONLY) RESULTS
# [TWO_SAMPLE_TEST (z-statistic ONLY): testing significance between two categorical features by comparing the means of another numerical feature]
# Determine if two categorical features have statistically different means for another numerical feature

# Does being a member signify that trip_time is significantly different than being a casual user?

Categorical feature:
member_casual where variables are 'member' and 'casual'
Numerical feature:
trip_distance
+----------------------+-----------------------+-------------------+-------------------+-----------+-----------+----------------------+
|      samp1_mean      |      samp2_mean       |     samp1_std     |     samp2_std     | samp1_len | samp2_len | z_critical_twosample |
+----------------------+-----------------------+-------------------+-------------------+-----------+-----------+----------------------+
| 0.006629366046007352 | 0.0037317409782921538 | 7.410616331577896 | 5.143858840641462 |  23815154 |  11647954 |   1.3543319887486756 |
+----------------------+-----------------------+-------------------+-------------------+-----------+-----------+----------------------+
NOT Significant

Numerical feature:
trip_time
+------------------+--------------------+--------------------+-------------------+-----------+-----------+----------------------+
|    samp1_mean    |     samp2_mean     |     samp1_std      |     samp2_std     | samp1_len | samp2_len | z_critical_twosample |
+------------------+--------------------+--------------------+-------------------+-----------+-----------+----------------------+
| 769.115660054099 | 2118.0848747342297 | 10493.561789157968 | 26302.32146198596 |  23815154 |  11647954 |  -168.59853112910267 |
+------------------+--------------------+--------------------+-------------------+-----------+-----------+----------------------+
Significant

Numerical feature:
birthyear_INT
+-------------------+--------------------+-------------------+------------------+-----------+-----------+----------------------+
|    samp1_mean     |     samp2_mean     |     samp1_std     |    samp2_std     | samp1_len | samp2_len | z_critical_twosample |
+-------------------+--------------------+-------------------+------------------+-----------+-----------+----------------------+
| 1981.194184606503 | 1987.4188076871399 | 10.90212715989175 | 9.73753096937569 |  23815154 |  11647954 |  -1717.7515076798247 |
+-------------------+--------------------+-------------------+------------------+-----------+-----------+----------------------+
Significant



# 3. Question 1: Why would casual riders buy Cyclistic annual memberships?

percentage/ 
+---------------+---------------+--------+----------------------+
| member_casual | rideable_type | gender |         f0_          |
+---------------+---------------+--------+----------------------+
| casual        | NULL          | Female | 0.004766284784788248 |
| casual        | NULL          | Male   | 0.007837291891818123 |
| casual        | classic_bike  | NULL   |  0.07631614844398663 |
| casual        | docked_bike   | NULL   | 0.053838365956834694 |
| casual        | electric_bike | NULL   |  0.08795399798808465 |
| member        | NULL          | Female |  0.11351653194069242 |
| member        | NULL          | Male   |    0.341074048199945 |
| member        | classic_bike  | NULL   |  0.13873474545997969 |
| member        | docked_bike   | NULL   |  0.05886766925239551 |
| member        | electric_bike | NULL   |    0.117094916081475 |
+---------------+---------------+--------+----------------------+

# From the onesample ttest, we learned that members closer to the mean of all bike users are members. If people become older/closer to the mean age of over all bike users, they are statistically likely to be members.

# people closer to the bike user mean age are likely to be members, and members are likely to be 6 years older than casual bike users

# 


