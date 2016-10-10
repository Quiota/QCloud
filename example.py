from s3_operator import S3Operator
import getpass

# input your credentials
key_id = getpass.getpass('id:')
key = getpass.getpass('key:')

# declare obejct
s3 = S3Operator(key_id, key)
s3.creat_client()

# select bucket

select_bucket = 'test.tf.quiota.com'


###############################################################################
# get filenames
s3.get_filenames(select_bucket)

# print filenames
for filename in s3.filenames:
	print filename
###############################################################################


###############################################################################
# upload single file
local_filename = './test.txt'
cloud_filename = 'test/test/test.txt'

s3.upload_single_file(local_filename, select_bucket, cloud_filename, 
    cloud_storage_class="STANDARD")
###############################################################################


###############################################################################
# download single file2
local_filename = './test/test1/test.txt'
cloud_filename = 'test/test/test.txt'
s3.download_single_file(local_filename, select_bucket, cloud_filename)
###############################################################################


###############################################################################
# upload multiple files
local_path = './test/'
cloud_folder_name = 'test'

s3.upload_multiple_files(local_path, select_bucket, 
    cloud_folder_name, cloud_storage_class="STANDARD")
###############################################################################


###############################################################################
# download multiple files
s3.get_filenames(select_bucket)
conditions = ['/test1/']
select_files = [f for f in s3.filenames if any(condition in f for condition in conditions)]

local_path = './'
s3.download_multiple_files(select_files, local_path, select_bucket)
###############################################################################
