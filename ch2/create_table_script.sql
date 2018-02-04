CREATE TABLE IF NOT EXISTS MOBILE_TABLE (
  id int,
  sub_id String,
  start_date_time timestamp,
  cell_id int, 
  location_id String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/ch2'