
# ---------------------------------------------
# Agreed, need to make a new table that is condensed with useful featueres
# ---------------------------------------------


0. combine ride_id and trip_id

SELECT IF(ride_id=NULL, trip_id, IF(trip_id=NULL, ride_id, NULL)) AS fin_trip_ID




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

