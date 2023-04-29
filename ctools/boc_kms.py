#!/usr/bin/env python3

"""
Manage the Secrets system, which uses a single encrypted file called ../secrets.json.encrypted.

"""
import os
import os.path
import json
import sys
import subprocess
import boto3
import botocore.config
from typing import Dict

THIS_DIR   = os.path.dirname( os.path.abspath( __file__))
PARENT_DIR = os.path.dirname(THIS_DIR)

assert os.path.exists(THIS_DIR)

for d in [THIS_DIR, PARENT_DIR]:
    assert os.path.exists(d),f"{d} does not exist"
    if d not in sys.path:
        sys.path.append(d)


import aws as aws
import json_enhanced as json_enhanced

# Note botocore now does not like https:// in proxy definition
# https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html#botocore.config.Config
def kms_client():
    http_proxy=os.environ['BCC_HTTPS_PROXY']
    if http_proxy and len(http_proxy) > 0:
        proxy_config = botocore.config.Config(proxies={'https':os.environ['BCC_HTTPS_PROXY'].replace("https://",""),
                                                       'http':os.environ['BCC_HTTP_PROXY'].replace("http://","")})
        return boto3.client("kms", config=proxy_config)
    else:
        return boto3.client("kms", config=botocore.config.Config())

def get_keyid() -> str:
    env = os.environ['DAS_ENVIRONMENT']
    filename = '/mnt/gits/das-vm-config/das_config.json'
    with open(filename, 'r') as f:
        j = json.loads(f.read())
        keyid = j[env]['KEYID']
    return keyid


def encrypt(keyid: str, plaintext: str) -> str:
    """Encrypt a buffer with our key and return"""
    response = kms_client().encrypt(KeyId=keyid, Plaintext=plaintext)
    return response['CiphertextBlob']


def decrypt(ciphertext: str) -> str:
    response = kms_client().decrypt(CiphertextBlob=ciphertext)
    return response['Plaintext']


def get_encrypted_json_file(filename: str) -> Dict:
    with open(filename,"rb") as f:
        out = decrypt(f.read())
        return json.loads(out)

def main(args):
    SECRETS_FILE = os.path.join(PARENT_DIR,"secrets.json.encrypted.")
    DECRYPT_FILE = os.path.join(PARENT_DIR,"secrets.json.encrypted.decrypted")

    from argparse import ArgumentParser,ArgumentDefaultsHelpFormatter
    parser = ArgumentParser( formatter_class = ArgumentDefaultsHelpFormatter,
                             description="Access the Secrets system" )
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--decrypt", action='store_true')
    g.add_argument("--decryptForEditing", action='store_true', help=f'Erase {DECRYPT_FILE} and decrypt {SECRETS_FILE} to {DECRYPT_FILE}')

    parser.add_argument("infile", help='specifies input file', nargs='?')
    parser.add_argument("outfile", help='specifies output file. Use "-" for stdout. If a file, must not exist.', nargs='?')

    args = parser.parse_args(args)

    if args.decryptForEditing:
        if not os.path.exists(SECRETS_FILE):
            raise FileNotFoundError(SECRETS_FILE)
        if os.path.exists(DECRYPT_FILE):
            os.unlink(DECRYPT_FILE)
        args.infile = SECRETS_FILE
        args.outfile = DECRYPT_FILE
        args.decrypt = True

    if not os.path.exists(args.infile):
        raise FileNotFoundError(args.infile)
    if args.outfile=="-":
        args.outfile="/dev/stdout"
    else:
        if os.path.exists(args.outfile):
            raise FileExistsError(args.outfile)


    with open(args.infile,"rb") as infile:
        # Read the data before creating the output file.
        # Prevents creating null files
        if args.decrypt:
            msg = decrypt(infile.read())
        else:
            raise RuntimeError("must specify --decrypt")

        with open(args.outfile,"wb") as outfile:
            outfile.write(msg)
            outfile.flush()
            subprocess.check_call(['ls','-l',outfile.name])

if __name__ == "__main__":
    main(sys.argv[1:])
