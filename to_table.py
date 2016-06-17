#!/usr/local/python-3.4.1/bin/ipython3
'''
$Author$
$Source$
$Revision$
$Date$
$Id$ 

Purpose:
'''

import os,sys
import re
import tempfile
import time
from datetime import timedelta, datetime
import pandas as pd
import numpy as np
import threading
sys.path.append(os.path.join(os.environ['HOME'], 'python_lib'))

t = time.time()
from utilities import getDbConnection
from IBLog import IBLog

scriptName = os.path.splitext(os.path.basename(__file__))[0]
logger = IBLog("{}.log".format(scriptName))
logger.init("Start {}.py...".format(scriptName))

dbtypes = {'mysql': {'DATE': 'DATE', 'DATETIME': 'DATETIME', 'INT': 'INT', 'FLOAT': 'FLOAT', 'VARCHAR': 'VARCHAR'},
           'oracle': {'DATE': 'DATE', 'DATETIME': 'DATE', 'INT': 'NUMBER', 'FLOAT': 'NUMBER', 'VARCHAR': 'VARCHAR2'}
           }

def main():
    

    
    inputFile = '/home/users/mhristov/tmp/acct_sync_pos.ib.dat'
    
    df = pd.read_csv(inputFile, sep='|')
    df['dt'] = datetime.now()
    df.info()
    df.rename(columns=lambda x: re.sub('^#', '', x), inplace=True)

    conn = getDbConnection('MYSQLDEV', schema='clams')
#     conn = getDbConnection('ORAQAIBCUST')
    
    write_frame(df, 'test', conn, 'mysql', if_exists='append')
    
    
    s = time.time() - t
    print('Time taken: {}'.format(timedelta(seconds=s)))
    conn.close()
    
def get_schema(frame, name, flavor):
    types = dbtypes[flavor]
#     print(types)
    column_types = []
    
    dtypes = frame.dtypes
#     print(dtypes)
    
    for i,k in enumerate(dtypes.index):
        dt = dtypes[k]
#         print(i, k, dt, dt.type)
#         print(str(dt.type))
        if str(type(dt.type)) == '<class numpy.datetime64>':
            sqltype = types['DATETIME']
        elif issubclass(dt.type, np.datetime64):
            sqltype = types['DATETIME']
        elif issubclass(dt.type, (np.integer, np.bool)):
            sqltype = types['INT']
        elif issubclass(dt.type, np.floating):
            sqltype = types['FLOAT']
        else:
            sampl = frame[frame.columns[i]][0]
#             print('sample', sampl)
            if str(type(sampl)) == '<class datetime.datetime>':
                sqltype = types['DATETIME']
            elif str(type(sampl)) == '<class datetime.date>':
                sqltype = types['DATE']
            else:
                if flavor in ('mysql', 'oracle'):
                    size = 2 + max(len(str(a)) for a in frame[k])
                    size *= 2
                    if size > 4000:
                        size = 4000
#                     print(k, 'varchar size', size)
                    sqltype = types['VARCHAR'] + '({})'.format(size)
                else:
                    sqltype = types['VARCHAR']
        column_types.append((k, sqltype))
#     print(['{0} {1}'.format(*x) for x in column_types])
    columns = ', \n '.join(['{0} {1}'.format(*x) for x in column_types])
    template_create = '''CREATE TABLE {name} ({columns});'''.format(**{'name': name, 'columns': columns})
    print(template_create)
    return template_create

def table_exists(name=None, con=None, flavor='oracle'):
    if flavor == 'sqlite':
        sql = "SELECT name FROM sqlite_master WHERE type='table' AND name = '{}';".format(name)
    elif flavor == 'mysql':
        sql = "show tables like '{}';".format(name)
    elif flavor == 'oracle':
        sql = "select table_name from user_tables where table_name='{}'".format(name.upper())
    else: raise NotImplementedError
    
    df = pd.read_sql(sql, con)
    print(sql, df)
    print('table exists?', len(df))
    exists = True if len(df) > 0 else False
    return exists

def split_into_files(frame):
    files = []
    dfs = np.array_split(frame, 4)
    for df in dfs:
        tempFile = tempfile.NamedTemporaryFile()
        df.to_csv(tempFile.name, index=False)
        files.append(tempFile)
    return files

def write_frame(frame, name=None, con=None, flavor='oracle', if_exists='fail'):
    '''
    Write a dataframe stored in a temp file to dbms
    
    if_exists:
        'fail': create table will be attempted and fail
        'replace': if table with name exists it will be deleted
        'append': assume table with correct schema exists and add data. If no table or bad data then fail
    If table doesn't exists it will be created
    '''

    print('dbstuff')
    print(con)
    if if_exists=='replace' and table_exists(name, con, flavor):
        cur = con.cursor()
        cur.execute("drop table {}".format(name))
        cur.close()
        
    cur = con.cursor()    
    if if_exists in ('fail', 'replace') or (if_exists == 'append' and table_exists(name, con, flavor) == False):
        #create table
        print(table_exists(name, con, flavor))
        schema = get_schema(frame, name, flavor)
        if flavor == 'oracle':
            schema = schema.replace(';', '')
        
        cur.execute(schema)
        print('Table {} created.'.format(name))
    
    tempFiles = split_into_files(frame)
    print(datetime.now())
    
    if flavor == 'mysql':
        print('Start loading data...')
        thread_list = []
        for tempFile in tempFiles:
            t = threading.Thread(target=load_data, args=(tempFile, name, flavor))
            thread_list.append(t)
        
        for thread in thread_list:
            thread.start()
        
        for thread in thread_list:
            thread.join()

    print(datetime.now(), 'Done')

def load_data(tempFile, tableName, flavor):
    db = getDbConnection('MYSQLDEV', schema='clams')
    cur = db.cursor()
    loadSql = '''LOAD DATA LOCAL INFILE '{}' INTO TABLE {}
                    FIELDS TERMINATED BY ','
                    OPTIONALLY ENCLOSED BY '"'
                    LINES TERMINATED BY '\n'
                    IGNORE 1 LINES'''.format(tempFile.name, tableName)
    print(loadSql)
    cur.execute(loadSql)
    db.commit()
    db.close()
    tempFile.close()  

if __name__ == "__main__":
    main()