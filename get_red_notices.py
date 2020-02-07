from hdbcli import dbapi

import urllib.request
import json
import argparse
import datetime
import os
import time

parser = argparse.ArgumentParser(
    description='Creates SAP HANA Tables and inserts the content of Interpol red notices in them')

parser.add_argument('--user', required=True, help='User for SAP HANA connection')
parser.add_argument('--password', required=True, help='Password for SAP HANA connection')
parser.add_argument('--host', required=True, help='SAP HANA server')
parser.add_argument('--port', required=True, type=int, help='Port to SAP HANA tenant')
parser.add_argument('--schema', help='Schema inside SAP HANA tenant', default='POLER')
parser.add_argument('--haas', help='Connect to Hana As a Service', action='store_true')

args = parser.parse_args()

# To make the query look like it's coming from a web browser.
# The user agent is currently not used.
user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 " \
             "Safari/537.36 "

url_red_notice = 'https://ws-public.interpol.int/notices/v1/red?&resultPerPage=500'

# TO DO ! Fix date parsing. This doesn't work
def parseDate(s: str):
    d = None
    try:
        d = datetime.datetime.strptime(date_time_str, '%Y/%m/%d')
    finally:
        return d


def gethanaconnection(arg) -> dbapi.Connection:

    if arg.haas is not None:
        certpath = os.environ['HOME'] + '/.ssl/DigiCertGlobalRootCA.pem'
        conn: dbapi.Connection = dbapi.connect(address=arg.host, port=arg.port, user=arg.user, password=arg.password,
                                         encrypt='true',
                                         sslCryptoProvider='openssl',
                                         sslTrustStore=certpath
                                         )
    else:
        conn: dbapi.Connection = dbapi.connect(address=arg.host, port=arg.port, user=arg.user, password=arg.password)
    cursor = conn.cursor()
    cursor.execute("SET SCHEMA "+arg.schema)
    cursor.close()
    print("Connection to SAP HANA successful !") if conn.isconnected() else print("Not connected")

    return conn


#POLER data model has 5 tables plus 5 attrbute tables, that is 10 sequences
seq = ['OBJ', 'PER', 'LOC', 'REL', 'EVT']
sqls = {}
for t in seq:
    sqls[t] = 'select SEQ_'+t+'.nextval from DUMMY'
    sqls[t+'_ATTR'] = 'select SEQ_'+t+'_ATTR.nextval from DUMMY'


def getobjectseq(conn: dbapi.Connection, obj: str) -> int:
    """Returns a sequence number for a given object"""
    cursor = conn.cursor()
    res = cursor.execute(sqls[obj])
    for row in cursor:
        return row[0]


def addper(conn: dbapi.Connection,  obj ):
    SQL_INSERT = """INSERT INTO PERSON(person_id, fullname, firstname, lastname, gender, dob, src_system) 
        VALUES (?, ?, ?, ?, ?, ?, 'interpol-python-rednotice')"""
    person_id = 'PER-' + str(getobjectseq(myconn, 'PER'))
    print(obj)
    firstname = obj['forename']
    lastname  = obj['name']
    dob       = parseDate(obj['date_of_birth'])
    cursor = conn.cursor()
    cursor.execute(SQL_INSERT, (person_id, firstname+' '+lastname, firstname, lastname, obj['sex_id'], dob))

    for attr in obj['attributes']:
        if not obj['attributes'][attr] is None:
            addattr(conn, "PERSON", person_id, None, obj['category'], attr, obj['attributes'][attr])

    return person_id


# Interate over table list to create SQL statements to insert and select attributes
attr_list = ["OBJECT", "PERSON"]
sql_attr = {}
for e in attr_list:
    sql_attr[e] = {"insert":"INSERT INTO "+e+"""_ATTR(entity_id, attr_id, attr_parent_id, attr_name, attr_full_name, attr_type, attr_data_type, attr_string_value, attr_num_value, attr_date)
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ? )""",
                   "exists":"SELECT attr_id from "+e+"""_ATTR 
                   where attr_full_name= ? 
                   and (attr_string_value=? or attr_string_value is null) 
                   and (attr_num_value = ? or attr_num_value is null)"""}


# Function to add an attribute for a given entity type (PERSON, OBJECT, etc...)
def addattr(conn: dbapi.Connection, etype: str,
            entity_id: str, #link to object or person for which the attribute applies
            attr_parent_id: str, #link to the parent attribute
            category:str,
            attr: str,
            data: any):

    print('#addattr ', etype, entity_id, attr_parent_id, category, attr, data)
    short = etype[0:3]
    cursor = conn.cursor()

    attr_full_name = category + '/' + attr
    attr_data_type = None
    attr_string_value = str(data)
    attr_num_value = None
    attr_date = None
    attr_type = 'simple'
    attr_id   = None
    child_id  = None
    parent_type = short if attr_parent_id is None else short+'_ATTR'
    # In case of an array, add each element
    if isinstance(data, list):
        for e in data:
            attr_id = addattr(conn, etype, entity_id, None, category, attr, e)
    else:
        if isinstance(data, str):
            attr_data_type = "string"
        elif isinstance(data, int) or isinstance(data, float):
            attr_data_type = "number"
            attr_num_value = data
        elif isinstance(data, dict):
            attr_data_type = "dict"
            attr_string_value = attr
            attr_type = 'nested'
        else:
            print('NEW datatype ', type(data))

        # Check if an attribute with the same value already exists
        # For instance 'French' as attribute type language_spoken
        cursor.execute(sql_attr[etype]['exists'], (attr_full_name, attr_string_value, attr_num_value))
        res = cursor.fetchall()

        if len(res) == 1 and attr_full_name != "INT_RED_NOT/arrest_warrants" :
            attr_id = res[0][0]
            print("reusing attribute node", attr_id, attr_full_name, attr_string_value, attr_num_value )
        else:
            attr_id = short + '_ATTR-' + str(getobjectseq(myconn, short + '_ATTR'))
            cursor.execute(sql_attr[etype]['insert'], (entity_id, attr_id, attr_parent_id, attr, attr_full_name, attr_type, attr_data_type, attr_string_value, attr_num_value, attr_date))
            print("new attribute node ", etype, attr_id, 'parent:',attr_parent_id, '->',attr_string_value,attr_num_value)

        if isinstance(data, dict):
            for e in data:
                if not data[e] is None:
                    child_id = addattr(conn, etype, entity_id, attr_id, attr_full_name, e, data[e])
                    addrel(conn, entity_id, child_id, short+'_ATTR', short+'_ATTR', 'HAS SUB-ATTRIBUTES')
        else:
            addrel(conn, entity_id, attr_id, parent_type, short + '_ATTR', 'HAS ATTRIBUTE')

    return attr_id


def addobject(conn: dbapi.Connection,  obj ):
    """Adds an object and return the object id"""
    SQL_INSERT="INSERT INTO OBJECT(obj_id, category, obj_label, src_system) VALUES (?, ?, ?, 'interpol-python-rednotice')"
    print('adding object ',obj)
    obj_id='OBJ-'+str(getobjectseq(myconn, 'OBJ'))
    cursor = conn.cursor()
    cursor.execute(SQL_INSERT, (obj_id, obj['category'], obj['obj_label']))

    if 'attributes' in obj:
        for attr in obj['attributes']:
            #addobjectattr(conn, obj_id, None, obj['category'], attr, obj['attributes'][attr])
            attr_id=addattr(conn, "OBJECT", obj_id, None, obj['category'], attr, obj['attributes'][attr])
            #addrel(conn, obj_id, attr_id,'OBJ', 'OBJ_ATTR','OBJECT HAS')

    return obj_id


#used to differentiate red notice attribute and person attributes
per_use_attr = ["forename", "name", "date_of_birth", "sex_id" ]
per_ignore_attr = ["arrest_warrants", "_links", "_embedded", "entity_id"]


def addnotice(conn: dbapi.Connection, notice: object):
    #print(notice)
    if '_links' in notice \
            and 'self' in notice['_links'] \
            and 'href' in notice['_links']['self']:
        reqn=urllib.request.urlopen(notice['_links']['self']['href'])
        nd = json.loads(reqn.read())
        obj = {"category": 'INT_RED_NOT',
               "obj_label": 'Red Notice/'+notice['forename']+' '+notice['name'],
               'attributes': {
                   "arrest_warrants": nd['arrest_warrants'],
                   "link": notice['_links']['self']['href']
               }}
        person_obj = {'attributes': {}}
        #copy person values
        for a in per_use_attr:
            person_obj[a] = nd[a]
            del nd[a]
        if '_links' in nd \
                and 'thumbnail' in nd['_links'] \
                and 'href' in nd['_links']['thumbnail']:
            #get the thumbnail
            person_obj['attributes']['thumbnail_url'] = nd['_links']['thumbnail']['href']

        #remove unwanted properties
        # use the remaining attributes for person
        for p in  nd:
            if nd[p] is None or p not in per_ignore_attr:
                person_obj['attributes'][p] = nd[p]

        person_obj['category'] = 'FUGITIVE'

        notice_id = addobject(conn, obj)
        person_id = addper(conn, person_obj)
        rel_id = addrel(conn, person_id, notice_id, 'PER', 'OBJ', 'WANTED FOR')
        rel_id = addrel(conn, notice_id, person_id, 'OBJ', 'PER', 'ISSUED FOR')
        conn.commit()


def addrel(conn: dbapi.Connection, src_id: str, dst_id: str, src_type: str, dst_type: str, rel_type: str) -> int:
    SQL_INSERT = """INSERT INTO RELATIONSHIP( rel_id, src_id,  dst_id, src_type, dst_type, rel_type, src_system) 
        values (?,?,?,?,?, ?,'interpol-python-rednotice')"""
    rel_id = getobjectseq(myconn, 'REL')
    print('Adding relation', src_id, '->', dst_id, '(',rel_type,')')
    cursor = conn.cursor()
    cursor.execute(SQL_INSERT, (rel_id,  src_id, dst_id, src_type, dst_type, rel_type))
    return rel_id


myconn = gethanaconnection(args)

page_addr = url_red_notice
while page_addr is not None:
    print('opening ', page_addr)
    req = urllib.request.urlopen(page_addr)
    resp = json.loads(req.read())
    for n in resp['_embedded']['notices']:
        addnotice(myconn, n)

    # Check for a next page:
    if '_links' in resp and 'next' in resp['_links']:
        page_addr = resp['_links']['next']['href']
        print('##############################################')
        # sleep 5 seconds before fetching next page
        time.sleep(5)
    else:
        page_addr = None

print('executing post load procedure')
curs = myconn.cursor()
curs.callproc("POLER.POST_LOAD")
myconn.commit()
curs.close()
print('finished')
