#!/usr/local/python-3.4.1/bin/python3
'''
Purpose: Tracks changes in the forex margin files: ['haircut_rates.dat', 'haircut_rates_ibca.dat', 'haircut_rates_nfa.dat']
'''

import os,sys
import re
import git
import pandas as pd
from datetime import datetime
from io import StringIO
import shutil
# from sqlalchemy.sql import table, column, select, update, insert
import sqlalchemy as sa
sys.path.append(os.path.join(os.environ['HOME'], 'python_lib'))

from utilities import getDbConnection
from IBLog import IBLog #@UnresolvedImport
from IBMail import IBMail

scriptName = os.path.splitext(os.path.basename(__file__))[0]
logger = IBLog("{}.log".format(scriptName))
logger.init("Start {}.py...".format(scriptName))

mail = IBMail()

def main():
    engine = getDbConnection('ORADEVIBCUST', asEngine=True, echo=False)
    dtls = mail.getEmailDetails('SENDTOMHRISTOV')
    
    dt = datetime.now().date()
    srcDir = '/home/users/mhristov/data/creditman'
    dstDir = '/home/users/mhristov/tmp/forexMarginChange'
    files = ['haircut_rates.dat', 'haircut_rates_ibca.dat', 'haircut_rates_nfa.dat']
 
    #initial data population run only once
    # populateTableInitialData(engine, dstDir, files)
    # sys.exit()  
    
#     copyFiles(srcDir, path, files)

    repo = git.Repo(dstDir)
    filesNew = getFilesContent(dstDir, files)
    filesOld = getCommittedFilesContent(repo, files)
    
    filesToCommit = [file for (file, df) in filesNew.items() if not df.equals(filesOld[file])]
    
    dfAllNew = None
    dfAllOld = None
    
    if len(filesToCommit) > 0:
        dfAllNew = pd.concat([filesNew[f] for f in filesToCommit])
        dfAllOld = pd.concat([filesOld[f] for f in filesToCommit])

        merged = mergeOldAndNew(dfAllOld, dfAllNew, dt)
        #Save changed files to the table forex_margin_changes and return dataframe with info to be send out
        toSendDf = updateTable(engine, 'forex_margin_changes', merged)
        #send out notification
        print(toSendDf)
#         sendNotification(dtls, toSendDf, dt)
        #Commit changed files
        commitChangedFiles(repo, filesToCommit, dt)
    else: 
        print('Old files and new files are same. Nothing to commit')


def getCommittedFilesContent(repo, files):
    commitedFilesContent = dict()
    for commit in repo.iter_commits(max_count=1):
        for blob in commit.tree.blobs:
            commitedFilesContent[blob.name] = blob.data_stream.read()
    for file in files:
        fileStr = StringIO(commitedFilesContent[file].decode())
        df = pd.read_csv(fileStr, delim_whitespace=True, comment='#', header=None, names=['curr1', 'curr2', 'margin'])
        df['MarginSource'] = file
        commitedFilesContent[file] = df
    return commitedFilesContent

def getFilesContent(path, files):
    filesContent = dict()
    for file in files:
        df = pd.read_csv('{}/{}'.format(path, file), delim_whitespace=True, comment='#', header=None, names=['curr1', 'curr2', 'margin'])
        df['MarginSource'] = file
        filesContent[file] = df
    return filesContent
    
def populateTableInitialData(engine, path, files):
    #run just once
    filesNew = getFilesContent(path, files)
    dfAllNew = pd.concat([filesNew[f] for f in filesNew])
    dt = datetime.now().date()
    dfAllNew['EffectiveFromDt'] = dt
    print(dfAllNew.sample(10))
    dfAllNew.columns = dfAllNew.columns.str.lower()
    dfAllNew.info()
    dfAllNew.to_sql('forex_margin_changes', engine, if_exists='append', index=False, chunksize=100)
    
def copyFiles(src, dst, files):
    for file in files:
        shutil.copy2(os.path.join(src, file), dst)
    print('Copy Done.')
    
def mergeOldAndNew(dfAllOld, dfAllNew, dt):
    merged = dfAllOld.merge(dfAllNew, how='outer', on=list(dfAllNew.columns), indicator=True)
    merged = merged.pivot_table(index=['MarginSource', 'curr1', 'curr2'], columns=['_merge'], values=['margin'], aggfunc='first')
    merged[('margin', 'dt')] = dt
    #remove null values rows created from the pivot table
    redundantRowsFilter = ~((merged[('margin', 'left_only')].isnull()) & (merged[('margin', 'right_only')].isnull()) & (merged[('margin', 'both')].isnull()))
    merged = merged[redundantRowsFilter]
    #remove unchanged rows. We are not interested in them
    notChangedRowsFilter = merged[('margin', 'both')].isnull()
    merged = merged[notChangedRowsFilter]
    return merged   

def updateTable(engine, tableName, df):
    conn = engine.connect()
    metadata = sa.MetaData(bind=engine)
    forexMarginChanges = sa.Table(tableName, metadata, autoload=True)
    trans = conn.begin()
    try:
        #Changed margin value: update on value and date needed
        changedValuesMask = (df[('margin', 'left_only')].notnull()) & (df[('margin', 'right_only')].notnull())
        changedValuesDf = df[changedValuesMask]
        if len(changedValuesDf) > 0:
            print('update on value and date needed')
#             print(changedValuesDf)
            for index, row in changedValuesDf.iterrows():
                u = sa.sql.update(forexMarginChanges)
#                 print(row[('margin', 'right_only')])
#                 print(row['dt'][0])
#                 print(index[0])
                u = u.values({'margin': row[('margin', 'right_only')], 'effectivefromdt': row[('margin', 'dt')]})
                u = u.where((forexMarginChanges.c.marginsource == index[0]) & (forexMarginChanges.c.curr1 == index[1]) & (forexMarginChanges.c.curr2 == index[2]))
#                 print(u)
                conn.execute(u)
        #New row not in old: insert needed
        newNotInOldMask = (df[('margin', 'left_only')].isnull()) & (df[('margin', 'right_only')].notnull())
        newNotInOldDf = df[newNotInOldMask]
        if len(newNotInOldDf) > 0:
            print('insert needed')
#             print(newNotInOldDf.columns)
            for index, row in newNotInOldDf.iterrows():
                i = sa.sql.insert(forexMarginChanges)
#                 print(row[('margin', 'right_only')])
#                 print(row['dt'][0])
                i = i.values({'marginsource': index[0], 'curr1': index[1], 'curr2': index[2], 'margin': row[('margin', 'right_only')], 'effectivefromdt': row[('margin', 'dt')]})
#                 print(i)
                conn.execute(i)
        #EffectiveToDate
        #Old row not in new file: update date needed
        oldNotInNewMask = (df[('margin', 'left_only')].notnull()) & (df[('margin', 'right_only')].isnull())
        oldNotInNewDf = df[oldNotInNewMask]
        if len(oldNotInNewDf) > 0:
            print('update effective to date needed')
            print(oldNotInNewDf)
            for index, row in oldNotInNewDf.iterrows():
                u = sa.sql.update(forexMarginChanges)
                u = u.values({'effectivetodt': row[('margin', 'dt')]})
                u = u.where((forexMarginChanges.c.marginsource == index[0]) & (forexMarginChanges.c.curr1 == index[1]) & (forexMarginChanges.c.curr2 == index[2]))
#             print(u)
            conn.execute(u)
    except:
        trans.rollback()
        raise
#     trans.commit()
    conn.close()
    # return data to be send as notification
    return df[(changedValuesMask) | (newNotInOldMask)]

def sendNotification(dtls,toSendDf, dt):
    if len(toSendDf) > 0:
        toSendDf.columns = toSendDf.columns.droplevel()
        del(toSendDf['both'])
        toSendDf.reset_index(inplace=True)
        toSendDf = toSendDf.rename(columns={'curr1': 'Curr1', 'curr2': 'Curr2','left_only': 'OldRate', 'right_only': 'NewRate', 'dt': 'EffectiveFromDate'})
        dtls['Body'] = toSendDf.to_csv(sep='|', index=False)
        dtls['Subject'] = 'Track Margin Changes {}'.format(dt)
#         mail.DoMailFromConfig(dtls,[])
        print(toSendDf.to_csv(sep='|', index=False))
        
def commitChangedFiles(repo, filesToCommit, dt):
    index = repo.index
    index.add(filesToCommit)
    index.commit('Changed files committed on {}'.format(dt))
    print('Changed files committed on {}'.format(dt))
    
    
if __name__ == "__main__":
    main()