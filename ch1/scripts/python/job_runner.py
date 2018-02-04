from sub_trans_data_processing import *

input_file_path = "MobileProtocol.20170327T221500.27784.udr"
dpiDuplicates_output_path = "dpiDuplicates"
CSV_output_path = "CSV_OUT"
dpiRejection_output_path = "dpiRejection"

sub_trans_data_processing(input_file_path, CSV_output_path,dpiDuplicates_output_path, dpiRejection_output_path)