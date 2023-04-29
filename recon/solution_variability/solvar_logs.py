import boto3
import argparse
import re
from typing import Optional, Dict
import pandas as pd


class LogFileIterator:
    """log iterators allows one to iterate through the filename AND body of a S3 fs tree"""
    def __init__(self, bucket_name: str, prefix: Optional[str] = None, filter_string: Optional[str] = None):
        self.s3 = boto3.client('s3')
        self.bucket = bucket_name
        self.prefix = prefix
        self.filter_str = filter_string

    def get_body(self, key: str):
        data = self.s3.get_object(Bucket=self.bucket, Key=key)
        return data['Body'].read().decode("utf-8")

    def __iter__(self):
        #result = self.s3.list_objects(Bucket = self.bucket, Prefix=self.prefix)
        paginator = self.s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.bucket, Prefix=self.prefix)
        for page in pages:
            if page['KeyCount'] > 0:
                for obj in page['Contents']:
                    key = obj.get('Key')
                    if self.filter_str is not None:
                        if re.match(self.filter_str, key) is not None:
                            body = self.get_body(key)
                            yield (key, body)
                    else:
                        body = self.get_body(key)
                        yield (key, body)
        """
        for obj in result:
            print(obj)
            key = obj.get('Key')
            if self.filter_str is not None:
                if re.match(self.filter_str, key) is not None:
                    body = self.get_body(key)
                    yield (key, body)
            else:
                body = self.get_body(key)
                yield (key, body)
        """

def process_file(name: str, body : str) -> Dict:
    """TODO: implement this"""
    return {
        'Block ID' : '000120324001213',
        'IV' : 3.0,
        'Population' : 3.0,
        'SIV' : 0.5,
        'Solve Time' : 34.51
    }


# collect the desired directory to process
ap = argparse.ArgumentParser("Solvar Log Summary Tool")
ap.add_argument("--bucket", default="uscb-decennial-ite-das", type=str)
ap.add_argument("--prefix", default="galois-solvar/solvar-out-block/", type=str)
ap.add_argument("--o", default="scaled_ivs.csv", type=str)
ap.add_argument("--nwrite", default=100, type=int)
a = ap.parse_args()

s3_bucket = a.bucket #'<uscb-thing>'
prefix = a.prefix #'/galois-solvar/<name>'
out_fname = a.o #'scaled_ivs.csv'
n_write = a.nwrite #1000

log_match = r'.*[0-9].log'
columns = ['Block ID', 'IV', 'Population', 'SIV', 'Solve Time']

print(f"Creating S3 Iterator...")
s3_iter = LogFileIterator(s3_bucket, prefix=prefix, filter_string=log_match)
rdf = pd.DataFrame(columns=columns, data=[])

print("Starting Iteration...")
for idx, (name, body) in enumerate(s3_iter):
    print(f"{idx}: ", name, f"{body[:20]}...")
    res = process_file(name, body)
    rdf = rdf.append(res, ignore_index=True)
    if (idx % n_write) == 0:
        print(f"{idx // n_write} Writing File...")
        rdf.to_csv(out_fname, index=False)
print("Finished.")
