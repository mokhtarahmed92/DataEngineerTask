## Sort the subscriber transaction by ID 
sort -t"," -k1 ch2_mock > sorted_mock

## subtract each tranaction timestamp with the transaction before it to calculate the period between them
## then sum all periods for the (subscriber, location ) key and set it into the array to be printed 
## the scrip is not completed yet and am still working on it 
awk -F',' '{ if (mktime(pre) != -1) arr2[$1, $4] += (mktime(pre) - mktime($2))/60} END { for (b in arr2) { print b , arr2[b]} }{pre=$2}' sorted_mock
