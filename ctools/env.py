"""Interface between environment variables, configuration variables, and Python.

Can read environment variables in bash scripts such as:

export FOO=bar
export BAR=biff

and return an associative array of {'FOO':'bar','BAR':biff}

Can find variables in /etc/profile.d/*.bash.

Can process JSON configuration files in the form:
  { "*": {'name':'val',...},
    "E1": {'name':'val',...},
    "E2": {'name':'val',...}}

and search for the value of 'name' going to the current environment (e.g. E1), and then to the default environemnt (e.g. *)
"""

import os
import re
import pwd
import sys
import glob
import json
import logging
from configparser import ConfigParser
from os.path import dirname,basename,abspath
from typing import Dict

VARS_RE   = re.compile(r"^(export)?\s*(?P<name>[a-zA-Z][a-zA-Z0-9_]*)=(?P<value>.*)$")
EXPORT_RE   = re.compile(r"^export \s*(?P<name>[a-zA-Z][a-zA-Z0-9_]*)=(?P<value>.*)$")

SRC_DIRECTORY = os.path.dirname(__file__)

# This determines whether we raise an error if we can't find a census env file
# There should be a way to figure this out that doesn't require an environment
require_census_env=os.getenv('REQUIRE_CENSUS_ENV')

def get_vars(fname: str,just_exports=True) -> Dict:
    """Read the bash EXPORT variables in fname and return them in a dictionary
    :param fname: the name of a bash script
    """
    ret = {}
    if just_exports:
        use_rx=EXPORT_RE
    else:
        use_rx=VARS_RE
    with open(fname, 'r') as f:
        for line in f:
            m = use_rx.search(line)
            if m:
                name  = m.group('name')
                value = m.group('value')
                if (len(value)>0) and (value[0] in ['"', "'"]) and (value[0]==value[-1]):
                    value = value[1:-1]
                ret[name] = value
    return ret


def get_env(pathname: str = None, *, profile_dir: str = None, prefix: str = None) -> Dict:
    """Read the BASH file and extract the variables. Currently this is
done with pattern matching. Another way would be to run the BASH
script as a subshell and then do a printenv and actually capture the
variables
    :param pathname: if provided, use this path
    :param profile_dir: If provided, search this directory
    :param prefix: if provided and profile_dir is provided, search for all files in the directory
    :return: the variables that were learned.
"""
    if (pathname is not None) and (profile_dir is not None):
        raise ValueError("pathname and profile_dir cannot both be provided")
    if (profile_dir is not None) and (prefix is None):
        raise ValueError("If profile_dir is provided, prefix must be provided.")
    if profile_dir:
        names = sorted(glob.glob(os.path.join(profile_dir, "???_" + prefix + "*")))
        if len(names)==0:
            names = sorted(glob.glob(os.path.join(profile_dir, prefix+"*")))
        if len(names)==0:
            raise FileNotFoundError(f"No file with prefix {prefix} in {profile_dir}")
        pathname = names[0]

    ret = {}
    if prefix is not None:
        just_exports=False
    else:
        just_exports=True
    for (key,val) in get_vars(pathname,just_exports=just_exports).items():
        ret[key] = os.path.expandvars(val)
    return ret


DAS_ENV_FILE=os.path.join(SRC_DIRECTORY,"census_das_env.sh")

if os.path.exists('/etc/profile.d/census_das.sh'):
    census_das_env=get_vars('/etc/profile.d/census_das.sh')
elif os.path.exists(DAS_ENV_FILE):
    census_das_env=get_vars(DAS_ENV_FILE)
elif require_census_env:
    raise FileNotFoundError(DAS_ENV_FILE)
else:
    logging.error("Can't find census env file /etc/profile.d/census_das.sh or %s"%DAS_ENV_FILE)
    census_das_env={}

def census_getenv(varname: str, fallback=None):
    if varname in census_das_env:
        return census_das_env[varname]
    elif os.environ.get(varname):
        return os.environ[varname]
    elif fallback is None:
        return os.environ[varname]
    else:
        return fallback

def import_census_env():
    if os.environ.get('CENSUS_DAS_ENV_INCLUDED'):
        return
    else:
        print("Importing census env")
        for (key,val) in census_das_env:
            if not os.environ.get(key):
                os.environ[key]=val
        print("env_s3root=%s"%os.getenv("DAS_S3ROOT"))

def get_home() -> str:
    """Return the current user's home directory without using the HOME variable. """
    return pwd.getpwuid(os.getuid()).pw_dir


def dump(out) -> None:
    print("==== ENV ====",file=out)
    for (key,val) in os.environ.items():
        print(f"{key}={val}",file=out)


class JSONConfigReader:
    @classmethod
    def searchFile(self, path: str) -> str:
        """Search for the file named by path in the current directory, and then in every directory up to the root.
        Then every directory from ctool's directory to root.
        When found, return it. Otherwise return path if the file exists, otherwise raise an exception
        """
        checked = []
        name = os.path.join( os.getcwd(), basename(path))
        while dirname(name) != '/':
            checked.append(name)
            if os.path.exists(name):
                return name
            name = os.path.join( dirname(dirname(name)), basename(name))

        name = os.path.join( dirname(abspath(__file__)), basename(path))
        while dirname(name) != '/':
            checked.append(name)
            if os.path.exists(name):
                return name
            name = os.path.join( dirname(dirname(name)), basename(name))


        if os.path.exists(path):
            return path
        for check in checked:
            logging.error(f"checked {check}")
        raise FileNotFoundError(path)

    def __init__(self, *, path: str = None, search: bool = True, config: ConfigParser = None,
                 environment=None, envar: str = None) -> None:
        """
        :param path: location of JSON config file.
        :param search: Search from current directory up to root for a file with the same filename as `path` before using `path`.
        :param config: If provided, use the configuration specified in this dictionary, instead of path.
        :param environment: Specifies the environment inside the JSON dictionary that should be used, and then default to 'default'.
        :param envar: Specifics an os.environ[] name that should be used for the environment.
        """
        self.environment= 'default'
        self.path = None
        if (path is not None) and (config is not None):
            raise ValueError("Only path or config can be specified")
        if (environment is not None) and (envar is not None):
            raise ValueError("Only environment or envar can be specified")
        if path:
            # If search is true, search for the named config file from the current directory to the root
            # directory. If it isn't found, use the pathname
            if search:
                self.path = self.searchFile(path)
            else:
                self.path = path
            self.config = json.load(open(self.path))
        else:
            self.path = 'provided dictionary'
            self.config = config
        if environment:
            self.environment = environment
        if envar:
            self.environment = os.environ[envar]

    def get_config(self, variable_name: str, environment=None) -> None:
        # Handle one layer deep of FOO.BAR to search in FOO's directory for BAR.
        if environment is None:
            environment = self.environment

        if "." in variable_name:
            (name,ext) = variable_name.split(".",1)
            val = self.get_config(name, environment)
            return val[ext]

        for check in [environment,'default']:
            try:
                return self.config[check][variable_name]
            except KeyError:
                pass
        print(f"config:\n{json.dumps(self.config,default=str,indent=4)}",file=sys.stderr)
        raise KeyError(f"{variable_name} not in {check} or 'default' in {self.path}")
