import json
import pytest
import os, sys
os.sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import boc_kms as boc_kms
from os.path import exists

@pytest.fixture(scope="session")
def tempdir(tmp_path_factory):
    """ Create a tmp directory for testing """
    tmpdir = tmp_path_factory.mktemp("boc_kms_test")
    return tmpdir


def create_test_file(tempdir):
    test_path = f'{tempdir}/test.txt'
    message = '{"TEST": "test_message"}'
    keyid = boc_kms.get_keyid()
    encrypted_text = boc_kms.encrypt(keyid, message)
    open(test_path, 'wb').write(encrypted_text)


def test_get_keyid():
    """ Check that the keyid can be retrieved """
    key = boc_kms.get_keyid()
    assert len(key) > 0, "keyid did not return value"

def test_create_kms_client(tempdir):
    """
    The kms_client is needed for running other specific functions, check it can be created
    """
    boc_kms.kms_client()

def test_kms_roundtrip():
    """
    use an encrypted string to test that boc_kms can correctly decrypt a message
    """
    test_message = 'test_message'
    keyid = boc_kms.get_keyid()
    encrypted_text = boc_kms.encrypt(keyid, test_message)

    # test boc_kms decrypt
    unencrypted_text = boc_kms.decrypt(encrypted_text).decode('utf-8')
    assert test_message == unencrypted_text

def test_kms_json_roundtrip(tempdir):
    """
    make encrypted_test_file and check that it can be opened and unencrypted
    """
    create_test_file(tempdir)
    test_path = f'{tempdir}/test.txt'
    unencrypted_json = boc_kms.get_encrypted_json_file(test_path)
    assert unencrypted_json['TEST'] == 'test_message'


def test_boc_kms_main(tempdir):
    """
    Run main function of boc_kms, decrypt file, then check decrypted file
    """
    create_test_file(tempdir)
    test_path = f'{tempdir}/test.txt'
    output_location = f'{tempdir}/output.txt'
    args = ['--decrypt', test_path, output_location]
    boc_kms.main(args)
    file = open(output_location, 'r')
    output = json.load(file)
    assert output['TEST'] == 'test_message'

