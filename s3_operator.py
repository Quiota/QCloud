import boto3
import os


class S3Operator(object):

    def __init__(self, aws_access_key_id, aws_secret_access_key):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

    def creat_client(self):
        # initialize client
        self.client = boto3.client('s3', 
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key)

    def get_filenames(self, select_bucket):
        # Create a reusable Paginator
        paginator = self.client.get_paginator('list_objects')
        # Create a PageIterator from the Paginator
        page_iterator = paginator.paginate(Bucket=select_bucket)
        # list all files and folder/files
        self.filenames = [content['Key'] for page in page_iterator 
            for content in page['Contents']]

    def upload_single_file(self, local_filename, select_bucket, cloud_filename,
        cloud_storage_class="STANDARD"):
        
        print 'uploading to bucket {0}'.format(select_bucket)
        print 'uploading to filename {0}'.format(cloud_filename)
        print 'uploading from {0}'.format(local_filename)
        
        # use STANDARD_IA for infrequent access files
        self.client.upload_file(local_filename, select_bucket, cloud_filename, 
            ExtraArgs={'StorageClass':cloud_storage_class})
    
    def download_single_file(self, local_filename, select_bucket, 
        cloud_filename):
        
        print 'downloading to bucket {0}'.format(select_bucket)
        print 'downloading from filename {0}'.format(cloud_filename)
        print 'downloading to {0}'.format(local_filename)

        local_path = os.path.join(*(local_filename.split('/')[:-1]))
        try:
            os.makedirs(local_path)
        except OSError as e:
            pass 

        self.client.download_file(select_bucket, cloud_filename, 
            local_filename)

    def upload_multiple_files(self, local_path, select_bucket, 
        cloud_folder_name, cloud_storage_class="STANDARD"):
        # upload multiple files
        # you need to decide what files to be uploaded
        select_files = []
        for root, dirs, filenames in os.walk(local_path):
            for filename in filenames:
                local_file_path = os.path.join(root.replace(local_path, ''), 
                    filename)
                select_files.append(local_file_path.lstrip('/'))

        for local_filename in select_files:
            cloud_filename = '{0}/{1}'.format(cloud_folder_name, 
                local_filename)
            print 'uploading to bucket {0}'.format(select_bucket)
            print 'uploading to filename {0}'.format(cloud_filename)
            print 'uploading from {0}'.format(os.path.join(local_path, 
                local_filename))
            
            # use STANDARD_IA for infrequent access files
            self.client.upload_file(os.path.join(local_path, 
                local_filename), select_bucket, cloud_filename, 
            ExtraArgs={'StorageClass':cloud_storage_class})
        
    def download_multiple_files(self, select_files, local_path, select_bucket):
        # download multiple files
        # you need to decide what files to be downloaded
        for cloud_filename in select_files:
            
            cloud_path_list = cloud_filename.split('/')
            local_file_path = os.path.join(local_path, *(cloud_path_list[:-1]))
            
            try:
                os.makedirs(local_file_path)
            except OSError as e:
                pass

            local_filename = os.path.join(local_file_path, cloud_path_list[-1])
            print 'downloading to bucket {0}'.format(select_bucket)
            print 'downloading from filename {0}'.format(cloud_filename)
            print 'downloading to {0}'.format(local_filename)
            self.client.download_file(select_bucket, cloud_filename, 
                local_filename)

    def delete_files(self, select_files, select_bucket):
        # incomplete !!!!!
        # delete objects
        objects_delete = [{'Key': filename} for filename in select_files]
        
        response = client.delete_objects(Bucket=select_bucket, 
            Delete={'Objects': objects_delete})

