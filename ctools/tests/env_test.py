import os
import sys
import io
from os.path import dirname,basename,abspath


sys.path.append(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import env

TEST_FILES_DIR = os.path.join(dirname(abspath(__file__)), "test_files")
TEST_BASH_FILE = os.path.join(TEST_FILES_DIR,"env_test.sh")

test_config = {"default": {"FOO":"default_foo",
                           "BAR":"default_bar"},
               "E1": {"FOO":"E1_foo",
                      "BAZ":"E1_baz",
                      "BINK":{"A":"a",
                              "B":"b" }}}

def test_get_env():
    ret = env.get_env(TEST_BASH_FILE)
    assert ret['FIRST']=='1'
    assert ret['FILE']=='the file'
    # env.get_env no longer side effects the global environment because it seemed like a bad idea
    #  that was likely to lead to misleading successes
    #assert os.environ['FIRST'] == '1'
    #assert os.environ['FILE'] == 'the file'

    ret2 = env.get_env(profile_dir=dirname(TEST_BASH_FILE), prefix='ctools')
    print(ret2)
    assert ret2['CTOOLS']=='YES'


def test_dump():
    os.environ['FOO']='BAR'
    f = io.StringIO()
    env.dump(f)
    assert "= ENV =" in f.getvalue()
    assert "FOO=BAR" in f.getvalue()


def test_searchFile():
    env_in_test_files_dir = os.path.join(TEST_FILES_DIR,"env.py")
    env_in_etc            = "/etc/env.py"
    assert env.JSONConfigReader.searchFile(env_in_test_files_dir) == abspath(env.__file__)
    assert env.JSONConfigReader.searchFile(env_in_etc) == abspath(env.__file__)
    assert env.JSONConfigReader.searchFile("/etc/motd") == "/etc/motd"


def test_JSONConfigReader():
    cr = env.JSONConfigReader(config=test_config)
    assert cr.get_config("FOO","E1")=="E1_foo"
    assert cr.get_config("BAR","E1")=="default_bar"
    assert cr.get_config("BAZ","E1")=="E1_baz"
    assert cr.get_config("FOO","E2")=="default_foo"
    assert cr.get_config("BAR","E2")=="default_bar"
    assert cr.get_config("BINK.A","E1")=="a"

    config_in_test_dir = os.path.join(TEST_FILES_DIR,"test_config.json")
    cr = env.JSONConfigReader(path=config_in_test_dir)
    assert cr.get_config("NAME","F") == "ALPHA"
