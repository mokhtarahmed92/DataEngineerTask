## count all records in the source files 
echo "count all records in the source files"
cat MobileProtocol.udr | wc -l 

### Get Random sample with 500 line from the source file 
shuf -n 5000 MobileProtocol.udr >  MobileProtocolSample.udr

## count all records in the sample  file
echo "count all records in the sample  file"
cat MobileProtocolSample.udr | wc -l 

### Ensure that all the measures in the sample are correct and match the source file and if it was wrong exclude it into another file
comm -2 -3 <(sort MobileProtocolSample.udr) <(sort MobileProtocol.udr) > wrong_measures.udr

## count all records in the wrong_measures file
echo "count all records in the wrong_measures file" 
cat wrong_measures.udr | wc -l 

### check the rejected records in the sampled file 
awk -F','  'BEGIN {OFS=","} { gsub(/ /, "", $8); if ($8 == "")  print }' MobileProtocolSample.udr > rejectedReocrdsSampled.udr


## count all records in the rejected records in the sample file 
echo "count all records in the rejected records in the sample file"
cat rejectedReocrdsSampled.udr | wc -l 


### check the rejected records in the source file 
awk -F','  'BEGIN {OFS=","} { gsub(/ /, "", $8); if ($8 == "")  print }' MobileProtocol.udr > rejectedReocrds.udr

## count all records in the rejected records in the source file 
echo "count all records in the rejected records in the source file "
cat rejectedReocrds.udr | wc -l 

read x