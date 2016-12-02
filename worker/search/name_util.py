import re
import string
"""

1. clean name  ( remove punctuations and non alpha characters )
2. create partial_names ( strip middle names  and suffix/prefixes/titles )
3. strip vowels ( prevents accented vowels from confusing the worker )

"""

CON_RE = re.compile("[BCDFGHJKLMNPQRSTVWXYZ]")
punctuation_map = dict((ord(char), None) for char in string.punctuation)

def get_search_key(name):

    #
    # single word names..Bono, Prince
    #
    if " " not in name:
        return None

    name = clean_name(name)
    if len(name) == 0:
        return None

    # contains only non-english characters
    if len(key(name,name)) == 0:
        return name 

    first, last, middle = get_partials(name)

    if last is None:
        return first
    
    return '.'.join([key(name,first),key(name,last)])


def clean_name(name):
    name = name.upper()

    if isinstance(name, str):
        name = name.translate(None, "0/123456789^.(){}<>-!_`',")

    elif isinstance(name, unicode):
        name = name.translate(punctuation_map)
    
    name = re.sub("^MR", "", name )
    name = re.sub("^MS", "", name )
    name = re.sub("^MRS", "", name )
    name = name.replace("-", "")

    name = name.strip()
    return name

def get_partials( name, remove_punc=False ):

    if remove_punc:
        name = clean_name(name)

    partial_names = re.split( "\s+", name )

    first_name = None
    last_name = None
    middle = None

    for suffix in ["MR", "MS", "SR", "JR", "II", "III", "IV"]:
        if suffix in partial_names:
            partial_names.remove(suffix)

    if len(partial_names) <= 1:
        return name, None, None

    first_name = partial_names[0]

    if len(partial_names) >= 2:
        last_name = partial_names[-1]

    if len(partial_names) > 2:
        middle = ''.join(partial_names[1:-1])

    return first_name, last_name, middle


def key(name,n):
    key = None
    if n[0] in ['A','E','I','O','U']:
        key =  n[0]+ ''.join( CON_RE.findall( n[1:]) )
    else:
        key = ''.join(CON_RE.findall( n ))

    if len(key) == 0:
        print name
        return n

    return key