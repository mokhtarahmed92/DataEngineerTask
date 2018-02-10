sort MobileProtocol.udr | uniq -d > script_duplicates.txt
echo "------------ number of duplicates in file ------------"
cat script_duplicates.txt | wc -l

comm -2 -3 <(sort script_duplicates.txt) <(sort true_duplicates.txt) > diff_duplicates_test1.txt
echo "------------ no. of diff duplicates in file testcase1  ------------"
cat diff_duplicates_test1.txt | wc -l


comm -2 -3 <(sort script_duplicates.txt) <(sort false_duplicates.txt) > diff_duplicates_test2.txt
echo "------------ no. of diff duplicates in file testcase2  ------------"
cat diff_duplicates_test2.txt | wc -l
