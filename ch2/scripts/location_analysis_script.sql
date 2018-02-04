SELECT sub_id,
	   location_id,
	   sum(stay_about_for_location) as total_duration,
	   max(stay_about_for_location) as max_consecutive_duration,
	   min(stay_about_for_location) as min_consecutive_duration
FROM(
	SELECT sub_id , location_id, sum(stay_about) as stay_about_for_location
	FROM(
		SELECT sub_id,
			   location_id,
			   group_id,
			   arrive_at,
			   leave_at,
			   (unix_timestamp(leave_at)- unix_timestamp(arrive_at))/60 as stay_about
		FROM(
		  SELECT sub_id,
				 location_id,
				 start_date_time as arrive_at,
				 COALESCE((Lead(start_date_time, 1) OVER(PARTITION BY sub_id ORDER BY start_date_time asc)), CAST("2017-03-10 13:00:00" as TIMESTAMP)) as leave_at,
				 (row_number() over (order by id) -row_number() over (partition by location_id order by id)) as group_id
		  FROM mobile_table
		  ) t
		) t1
	group by sub_id , location_id, group_id
	order by sub_id , location_id, group_id
) t3
group by sub_id , location_id
order by sub_id , location_id