# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.
import logging
from itertools import ifilter, imap

import boto3


logger = logging.getLogger(__name__)


class S3Store:

    def __init__(self, bucket_name):
        self.bucket = boto3.resource('s3').Bucket(bucket_name)

    def list_keys(self, prefix):
        return self.bucket.objects.filter(Prefix=prefix)

    def list_folders(self, prefix='', delimiter='/'):
        paginator = boto3.client('s3').get_paginator('list_objects')
        result = paginator.paginate(Bucket=self.bucket.name,
                                    Prefix=prefix,
                                    Delimiter=delimiter)
        for page in result:
            common_prefixes = page.get('CommonPrefixes')
            if common_prefixes:
                for item in common_prefixes:
                    yield item['Prefix']

    def get_key(self, key):
        try:
            return self.bucket.Object(key).get()['Body'].read()
        except:
            raise Exception('Error retrieving key "{}" from S3'.format(key))

    def upload_file(self, file_obj, prefix, name):
        key = ''.join([prefix, name])
        logger.info('Uploading file to {}:{}'.format(self.bucket.name, key))
        self.bucket.put_object(Key=key, Body=file_obj)

    def delete_key(self, key):
        self.bucket.Object(key).delete()

    def is_prefix_empty(self, prefix):
        result = self.bucket.objects.filter(Prefix=prefix).limit(1)
        return len(list(result)) == 0
