"""
@author: Weijian

self ordering list of Column Headers
"""

class Field(object):
    def __init__(self, name, colnum, letter, width ):
        self.name = name
        self.colnum = colnum
        self.letter = letter
        self.width = width

ALL_FIELDS = []


def _field(name='', width=20):
    """
    instantiates a Field and adds to the ALL_FIELDS
    """
    colnum = len(ALL_FIELDS)+1
    letter = colnum_string(colnum)    
    column = Field(name, colnum, letter, width)
    ALL_FIELDS.append(column)
    return column


def colnum_string(n):
    """
    gives the excel column representation of a column number
    i.e. 28 = AB
    """
    div = n
    string = ""
    temp = 0
    while div > 0:
        module = (div-1) % 26
        string = chr(65+module)+string
        div = int((div-module)/26)
    return string


NAME = _field(name="NAME", width=25)
FIRM = _field(name="FIRM", width=30)
REFERENCE_ID = _field(name="REFERENCE", width=10)
COUNTRY = _field(name="COUNTRY", width=5)
REGISTER = _field(name="REGISTER", width=5)
MATCH_NAME = _field(name="MATCH NAME", width=25)
MATCH_SCORE = _field(name="MATCH SCORE", width=5)
MATCH_REFERENCE_ID = _field(name="REFERENCE", width=10)
INDIVIDUAL_STATUS = _field(name="Active", width=10)
INDIVIDUAL_REGISTRATION_STATUS = _field(name="REGISTRATION STATUS", width=20)
LINK = _field(name="LINK", width=10)
MATCH_FIRM = _field(name="FIRM", width=30)
FIRM_EMPLOYMENT = _field(name="EMPLOYMENT", width=10)
FIRM_STATUS = _field(name="FIRM STATUS", width=20)

