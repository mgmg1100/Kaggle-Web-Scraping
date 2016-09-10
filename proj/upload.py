import boto
import boto.s3

import os.path
import sys

class Upload:
    def upload(sourceDir, destDir):
    # Fill these in - you get them when you sign up for S3
       AWS_ACCESS_KEY_ID = 'AKIAIMVB7CGPOS6XN53A'
       AWS_ACCESS_KEY_SECRET = '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'
       # Fill in info on data to upload
       # destination bucket name
       bucket_name = 'bia660-crawler'
       # source directory
       #sourceDir = 'C:\\Users\\Alapha\\Desktop\\test\\'
       # destination directory name (on s3)
       #destDir = 'abc/'
    
       #max size in bytes before uploading in parts. between 1 and 5 GB recommended
       MAX_SIZE = 20 * 1000 * 1000
       #size of parts when uploading in parts
       PART_SIZE = 6 * 1000 * 1000
    
       conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY_SECRET)
    
       bucket = conn.create_bucket(bucket_name,
               location=boto.s3.connection.Location.DEFAULT)
    
    
       uploadFileNames = []
       for (sourceDir, dirname, filename) in os.walk(sourceDir):
           uploadFileNames.extend(filename)
           break
    
       #print("filesNames")
       #print(uploadFileNames)
    
       def percent_cb(complete, total):
           sys.stdout.write('.')
           sys.stdout.flush()
    
       for filename in uploadFileNames:
           sourcepath = os.path.join(sourceDir + filename)
           destpath = os.path.join(destDir, filename)
           print('Uploading %s to Amazon S3 bucket %s' % \
                  (sourcepath, bucket_name))

           filesize = os.path.getsize(sourcepath)

           k = boto.s3.key.Key(bucket)
           k.key = destpath
           k.set_contents_from_filename(sourcepath,
                   cb=percent_cb, num_cb=10)
