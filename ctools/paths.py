"""
paths.py

A collection of functions useful for working with file, web, and AWS paths
"""

import os
import os.path
import re
try:
    from ctools.env import census_getenv
except ImportError:
    from das_framework.ctools.env import census_getenv

variable=re.compile('(\$\{\w*\})|(\$\w*)')

def substvars(string:str) -> str:
    """Replace environment variable references in string. This is different from 
       os.path.expandvars in two ways. First, it uses census_getenv which has its
       own table in addition to os.environ. Second, it doesn't default to the empty
       string but leaves unresolved variables, which helps with debugging.
    """
    start=0
    length=len(string)
    match=variable.search(string,start)
    if not match:
        return string
    result=''
    while match:
        match_start=match.start()
        match_end=match.end()
        # print('got match %s pos=%s end=%s start=%s'%(match,match.pos,match.endpos,start))
        if match_start > start:
            result += string[start:match_start]
        if string[match_start+1] == '{':
            varname=string[match_start+2:match_end-1]
        else:
            varname=string[match_start+1:match_end]
        substval=census_getenv(varname)
        # print('got substval %s for %s'%(substval,varname))
        if substval:
            result += substval
        else:
            result += string[match_start:match_end]
        start=match_end
        match=variable.search(string,start)

    if start < length:
        # print('at end, appending tail [%s:%s]'%(start,length))
        result += string[start:length]
    return result

def mkpath(prefix: str,relpath=None) -> str:
    if relpath:
        return os.path.join(substvars(prefix),substvars(relpath))
    else:
        return substvars(prefix)
