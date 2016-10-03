'''
Created on 7 Nov 2014

@author: mhristov
'''
import os
import re
import cx_Oracle
from time import time
from functools import wraps
from datetime import datetime, timedelta
from collections import defaultdict
from itertools import islice
from pandas import Series
import pyprind

def autodict(): return defaultdict(autodict)

def split_array(it, size):
    def into_chunks(it, size):
        '''Function for splitting arrays into chunks of given size'''
        it = iter(it)
        return iter(lambda: tuple(islice(it, size)), ())
    return [Series(s) for s in into_chunks(it, size)]

def timeit(method):
    @wraps(method) #using wraps to be able to preserve the docstring of the method
    def timed(*args, **kwargs):
        ts = time()
        result = method(*args, **kwargs)
        te = time()
        m, s = divmod(te - ts, 60)
        h, m = divmod(m, 60)
        print('[{}(*args, **kwargs)]'.format(method.__name__))
        print('[Execution time: {:0>2.0f}:{:0>2.0f}:{:0>2.0f}(h:m:s)]'.format(h, m, s))
        return result
    return timed

def getDbCredentials(dbalias, asEngineStr=False):
    '''Function to get database credentials from a config file ~/config/.dbaccess.config for a database alias
    If asEngineStr is true returns sqlalchemy engine connection string'''
    dbalias=dbalias.upper()
    config_file=os.environ['HOME']+"/config/.dbaccess.config"
    file = open(config_file)
    lines = file.readlines()
    data = []
    mysqlConfig = dict()
    for line in lines:
        if line.startswith('#'): continue
        m = re.match("^{0}\|".format(dbalias),line)
        if m is None:
            continue 
        data = line.strip().split('|')
        schema = data[1].upper()
        port = data[2]
        user = data[3]
        passwd = data[4]
        #for mysql only
        mysqlConfig['user'] = user
        mysqlConfig['password'] = passwd
        mysqlConfig['host'] = schema
        mysqlConfig['port'] = port
    if data:
        if dbalias.startswith('ORA'):
            if asEngineStr:
                return 'oracle://{0}:{1}@{2}'.format(user,passwd,schema)
            else:
                return '{0}/{1}@{2}'.format(user,passwd,schema)
        elif dbalias.startswith('MYSQL'):
            if asEngineStr:
                return 'mysql+mysqlconnector://{0}:{1}@{2}:{3}'.format(mysqlConfig['user'], mysqlConfig['password'], mysqlConfig['host'], mysqlConfig['port'])
            else:
                return mysqlConfig
    else:
        return "X"

def getDbConnection(dbAlias, schema=None, asEngine=False, echo=False):
    '''Returns connection object based on dbAlias.
    Schema argument is only applicable for mysql connections.
    If asEngine argument is set to True returns sqlalchemy engine.
    If echo is set to True makes the engine in echo mode    
    
    Usage: conn = getDbConnection('ORADEVIBCUST')
    
    '''
    conn = None
    dbCredentials = getDbCredentials(dbAlias)
    engineEcho = False

    if asEngine:
        from sqlalchemy import create_engine
        if echo:
            engineEcho = True
    if dbCredentials != 'X':
        if dbAlias.startswith('ORA'):
            if asEngine:
                connStr = 'oracle://{}'.format(dbCredentials.replace('/',':'))
                conn = create_engine(connStr, echo=engineEcho)
            else:
                conn = cx_Oracle.connect(dbCredentials)
        elif dbAlias.startswith('MYSQL'):
            import mysql.connector
            if asEngine:
                dbCredentials['schema'] = schema
                dbCredentials['allow_local_infile'] = True
                connStr = 'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{schema}'.format(**dbCredentials)
                conn = create_engine(connStr, echo=engineEcho)
            else:
                conn = mysql.connector.connect(database=schema,**dbCredentials)
    else: print('Error: dbAlias {} not valid!'.format(dbAlias))
    return conn

def rowsToDictList(cursor):
    columns = [i[0] for i in cursor.description]
    return [dict(zip(columns,row)) for row in cursor]

@timeit
def queryTradestoreFiles(pd, startDate, endDate, filter_=None, columns=None, skipZeroTrades=False, dropDuplicateTrades=False):
    '''Function to get data from tradestore files located at /home/users/csprod/ibcs/data/tradestore/IN/
    Params: startDate, endDate format YYYYMMDD
            filter example: filter = lambda df: df['CONTRA_FIRM_ID'] == ''
            columns example: list of column names ['CONTRA_FIRM_ID', 'EXCHANGE', 'CONTRACT_ID']
            
            #Pos Column Name
            0     EXEC_ID
            1     TRADE_DT
            2     TRADE_TM
            3     CONTRACT_ID
            4     CONTRACT_SYMBOL
            5     CONTRACT_TYPE
            6     QUANTITY
            7     PRICE_NUM
            8     PRICE_DEN
            9     EXCHANGE
            10    PARTIAL_FILL
            11    ORDER_DT
            12    ORDER_TM
            13    CLEARING_FIRM_ID
            14    CONTRA_FIRM_ID
            15    GIVEUP_FIRM_ID
            16    ORDER_ID
            17    USER_ID
            18    ORDER_ATTRIB
            19    EXEC_ATTRIB
            20    EXTERNAL_EXEC_ID
            21    PRICE
            22    IBCUSTACCT_ID
            23    CLIENT_ACCT_REF
            24    CLIENT_ORDER_REF
            25    PARENT_EXEC_ID
            26    CLEARING_MEMBER_ACCT
            27    ORDER_ATTRIB_HEX
            28    MISC1
            29    BLOCK_ID
            30    ORIGINAL_ECP_TSTAMP
            31    EXEC_ATTRIB_HEX
            32    ORDER_ATTRIB_STR
            33    EXEC_ATTRIB_HEX_STR
            34    THIInfoCol
            35    THIInfoCol
            36    THIInfoCol
            37    THIInfoCol
            38    THIInfoCol
            39    THIInfoCol
            40    THIInfoCol
            41    THIInfoCol
            42    THIInfoCol
            43    THIInfoCol
            44    THIInfoCol
    '''
    
    datelist = pd.date_range(start=pd.to_datetime(startDate, format='%Y%m%d'), end=pd.to_datetime(endDate, format='%Y%m%d'))
    dfs = list()
    for dt in datelist.tolist():
        file = '/home/users/csprod/ibcs/data/tradestore/IN/tradestore.alldata.{0:%Y%m%d}.gz'.format(dt)
        print('Parsing file: {}'.format(file))
        if os.path.exists(file):
            iter_csv = pd.read_csv(file, sep='|', compression='gzip', iterator=True, chunksize=100000)
            if filter_ == None:
                df = pd.concat([chunk for chunk in iter_csv])
            else:
                df = pd.concat([chunk[filter_(chunk)] for chunk in iter_csv])
                # old logic-> df = pd.concat([chunk[filter_(chunk)][columns] for chunk in iter_csv])
            
            #Remove trades with 0 quantity
            if skipZeroTrades:
                df = df[df['QUANTITY'] != 0]
            #Remove duplicating trades 
            if dropDuplicateTrades:
                df['EXEC_ID_SHORT'] = df['#EXEC_ID'].map(lambda x: '.'.join(x.split('.')[:3]))
                print('Found {} duplicated exec_ids. Flag dropDuplicateTrades = true so droping them.'.format(len(df[df['EXEC_ID_SHORT'].duplicated()])))
                df.drop_duplicates(['EXEC_ID_SHORT'], inplace=True)
                 
            if columns == None:
                dfs.append(df)
            else:
                dfs.append(df[columns])
        else:
            print('Warning: file doesn\'t exist!')
    dfAll = pd.concat(dfs)
    return dfAll

def convertSequenceToDict(list_):
    dic = {}
    argList = range(1,len(list_)+1)
    for k,v in zip(argList, list_):
        dic[str(k)] = v
    return dic

def writeDataFrame(conn, tableName, df, commit_=True):
    '''Function to write pandas dataframe to a table.
    The table must exists in the database. It is not created automatically.
    Currently the function does not support CLOBs.
    '''
    cur = conn.cursor()
    df = df.where(df.notnull(),None)
    cols = df.columns
    colnames = ', '.join(cols)
    colpos = ', '.join(':'+str(i+1) for i,f in enumerate(cols))
    
    insertSql = 'insert into {0} ({1}) values ({2})'.format(tableName, colnames, colpos)
    cur.prepare(insertSql)
    data = [convertSequenceToDict(rec) for rec in df.values]
    cur.executemany(None, data)
    print('{} rows inserted.'.format(cur.rowcount))
    if commit_:
        conn.commit()
    
def memory_usage():
    '''Function to get the memory used by the script this function is called from in MB '''
    import resource
    rusage_denom = 1024
    mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / rusage_denom
    return mem

@timeit
def getAcctsProperties(engineIbcust, acctSeries, columns):
    '''Function to get acct properties from different tables in the database based on a series/list of accts.
    Supported tables:
        CUSTOMERACCOUNT_RTAB, APPLICANT_RTAB, ACCOUNTHIERARCHY, CUSTOMERACCOUNTUSERS_RTAB, universalAccount_rtab
        accountCapability_rtab, REP_ACCT_FIN_SUMMARY, IBUSER_RTAB, INDIVIDUAL_RTAB, ACCT_CASH_BAL_SUMM
    Usage:
        df = getAcctsProperties(engineIbcust, acctSeries, columns)
        engineIbcust: sqlalchemy engine
        acctSeries: pd.Series/list of accts
        columns: list of desired columns from the supported tables
            dummy columns can be provided for calling various plsql function
            supported dummy columns:
                ['acct_type', 'acct_country', 'acct_region', 'is_stl', 'is_unreal', 'is_margin_acct', 'is_ecp']
    '''
    import pandas as pd
    import sqlalchemy as sa
    
    columns = list(map(str.lower, columns))

    tables = ['CUSTOMERACCOUNT_RTAB', 'APPLICANT_RTAB', 'ACCOUNTHIERARCHY',
              'CUSTOMERACCOUNTUSERS_RTAB', 'universalAccount_rtab', 'accountCapability_rtab',
              'ENTITYASSOC_RTAB', 'REP_ACCT_FIN_SUMMARY', 'IBUSER_RTAB', 'INDIVIDUAL_RTAB', 'ACCT_CASH_BAL_SUMM']
    tables = list(map(str.upper, tables))
    
    tableObjs = ('customerAccountObj', 'applicantObj', 'accountHierarchyObj',
                 'custAcctUsersObj', 'universalAcctObj', 'acctCapabilityObj',
                 'entityAssocObj', 'repAcctFinSumObj', 'ibUserObj', 'indivObj', 'cashBalSumObj')
    
    tableObjsDict = dict(zip(tableObjs, tables))

    # special columns not available in the supported tables
    dummyColumns = ['acct_type', 'acct_country', 'acct_region', 'is_stl', 'is_unreal', 'is_margin_acct', 'is_ecp']

    checkIfColumnsInTables(engineIbcust, tables, columns, dummyColumns)

#     print(tables)
    filteredTables = filterTablesByColumns(engineIbcust, tables, columns)
    # print(filteredTables)
    for k in tableObjsDict:
        if tableObjsDict[k] not in filteredTables:
            tableObjsDict[k] = None

#     print(tableObjsDict)
        
    allColumns = columns + ['acct_id', 'applicant_id', 'id', 'sub_acct_id']
  
#     print([c.name for c in customerAccount_rtab.columns])
    # add acct_id and applicant_id to the columns
    
    if type(acctSeries) == list:
        acctSeries = pd.Series(acctSeries)
    
    chunks = list()
    dfs = list()
#     if len(acctSeries) > 999 and len(acctSeries) < 1900:
#         chunks = array_split(acctSeries, 2)
#     elif len(acctSeries) >= 1900:
#         chunks = array_split(acctSeries, len(acctSeries)//800)
#     else:
#         chunks.append(acctSeries)
    chunks = split_array(acctSeries, 990)
    
    bar = pyprind.ProgBar(len(chunks), monitor=True, title='getAcctsProperties')
    for chunk in chunks:
        dfCustomerAccount = getDfFromTableObj(pd, sa, engineIbcust, tableObjsDict['customerAccountObj'], chunk, allColumns, columns)
        # dfCustomerAccount.head()
        dfApplicant = getDfFromTableObj(pd, sa, engineIbcust, tableObjsDict['applicantObj'], dfCustomerAccount.applicant_id, allColumns, columns)
        if dfApplicant is None:
            dfApplicant = pd.DataFrame(columns=['id'])
        dfAccountHierarchy = getDfFromTableObj(pd, sa, engineIbcust, tableObjsDict['accountHierarchyObj'], dfCustomerAccount.acct_id, allColumns, columns)
        if dfAccountHierarchy is None:
            dfAccountHierarchy = pd.DataFrame(columns=['sub_acct_id'])
        dfUniversalAccount = getDfFromTableObj(pd, sa, engineIbcust, tableObjsDict['universalAcctObj'], dfCustomerAccount.acct_id, allColumns, columns)
        if dfUniversalAccount is None:
            dfUniversalAccount = df = pd.DataFrame(columns=['acct_id'])
        dfAcctCapability = getDfFromTableObj(pd, sa, engineIbcust, tableObjsDict['acctCapabilityObj'], dfCustomerAccount.acct_id, allColumns, columns)
        if dfAcctCapability is None:
            dfAcctCapability = pd.DataFrame(columns=['acct_id'])
        dfIbUser = getDfFromTableObj(pd, sa, engineIbcust, tableObjsDict['custAcctUsersObj'], dfCustomerAccount.acct_id, allColumns, columns)
        if dfIbUser is None:
            dfIbUser = pd.DataFrame(columns=['acct_id'])
        dfIndividual = getDfFromTableObj(pd, sa, engineIbcust, tableObjsDict['entityAssocObj'], dfCustomerAccount.applicant_id, allColumns, columns)
        if dfIndividual is None:
            dfIndividual = df = pd.DataFrame(columns=['applicant_id'])
        dfRepAcctFinSum = getDfFromTableObj(pd, sa, engineIbcust, tableObjsDict['repAcctFinSumObj'], dfCustomerAccount.acct_id, allColumns, columns)
        if dfRepAcctFinSum is None:
            dfRepAcctFinSum = df = pd.DataFrame(columns=['acct_id'])
        dfCashBalSum = getDfFromTableObj(pd, sa, engineIbcust, tableObjsDict['cashBalSumObj'], dfCustomerAccount.acct_id, allColumns, columns)
        if dfCashBalSum is None:
            dfCashBalSum = df = pd.DataFrame(columns=['acct_id'])
        
        dfAll = dfCustomerAccount.merge(dfApplicant, how='left', left_on='applicant_id', right_on='id', suffixes=('', '_applicant'))
        dfAll = dfAll.merge(dfAccountHierarchy, how='left', left_on='acct_id', right_on='sub_acct_id')
        dfAll = dfAll.merge(dfIbUser, how='left', on='acct_id', suffixes=('','_ibUser'))
        dfAll = dfAll.merge(dfUniversalAccount, how='left', on='acct_id', suffixes=('','_universalAccount'))
        dfAll = dfAll.merge(dfAcctCapability, how='left', on='acct_id')
        dfAll = dfAll.merge(dfIndividual, how='left', on='applicant_id', suffixes=('','_individual'))
        dfAll = dfAll.merge(dfRepAcctFinSum, how='left', on='acct_id', suffixes=('','_rep_acct_fin_summary'))
        dfAll = dfAll.merge(dfCashBalSum, how='left', on='acct_id', suffixes=('','_acct_cash_bal_summ'))
        
#         dfAll.info()
        del(dfAll['id'])
        del(dfAll['sub_acct_id'])
        if 'applicant_id' not in columns: del(dfAll['applicant_id'])
        # if 'user_id' not in columns: del(dfAll['user_id'])
        if 'applicant_id_universalAccount' in dfAll.columns: del(dfAll['applicant_id_universalAccount'])
        if 'applicant_id_rep_acct_fin_summary' in dfAll.columns: del(dfAll['applicant_id_rep_acct_fin_summary'])
        dfs.append(dfAll)
#         print(dfs)
        bar.update()
    if len(dfs) > 0:
        df = pd.concat(dfs)
        df.drop_duplicates(inplace=True)
        df.columns = df.columns.str.upper()
        df = df.reset_index(drop=True)

    else:
        # if resultset is empty return empty dataframe with the parsed columns
        columns.insert(0, 'acct_id')
        df = pd.DataFrame(columns=[c.upper() for c in columns])

    print(bar)
    return df

def filterTablesByColumns(engineIbcust, tables, columns):
    # conn = engineIbcust.connect()
    sql = '''
    select distinct table_name from all_tab_columns 
    where table_name in ({})
    and column_name in ({})
    '''.format(','.join(["'{}'".format(t.upper()) for t in tables]), ','.join(["'{}'".format(c.upper()) for c in columns]))
    #print(sql)
    result = engineIbcust.execute(sql)
    tabs = [r[0] for r in result]
    if 'IBUSER_RTAB' in tabs:
        tabs.append('CUSTOMERACCOUNTUSERS_RTAB')    
    if 'INDIVIDUAL_RTAB' in tabs:
        tabs.append('ENTITYASSOC_RTAB')
    if 'CUSTOMERACCOUNT_RTAB' not in tabs:
        tabs.append('CUSTOMERACCOUNT_RTAB')
    if 'acct_country' or 'acct_region' in [c.lower() for c in columns]:
        tabs.append('APPLICANT_RTAB')
    # conn.close()
    return tabs

def checkIfColumnsInTables(engineIbcust, tables, columns, dummyColumns):
    sql = '''
    select count(1) cnt from all_tab_columns
    where table_name in ({})
    and column_name = upper(:1)
    '''.format(','.join(["'{}'".format(t.upper()) for t in tables]))

    unknownColumns = list()

    for column in columns:
        res = engineIbcust.execute(sql, column)
        cnt = res.fetchone()[0]

        if cnt == 0 and column not in dummyColumns:
            unknownColumns.append(column)
    if len(unknownColumns) > 0:
        raise Exception('Unknown column(s): {}'.format(unknownColumns))

def getDfFromTableObj(pd, sa, engineIbcust, tableName, series, allColumns, columns):
    df = None
    needsJoin = False
    
    selectedColumns = None
    colLength = 0
    
    metadata = sa.MetaData(bind=engineIbcust)

    if tableName in ['CUSTOMERACCOUNT_RTAB', 'UNIVERSALACCOUNT_RTAB', 'ACCOUNTCAPABILITY_RTAB']:
        tableObj = sa.Table(tableName.upper(), metadata, schema='ibcust', autoload=True)
        selectedColumns = [c for c in tableObj.columns if c.name in allColumns]
        colLength = len([c for c in tableObj.columns if c.name in columns])     
        if 'acct_type' in columns:
            colLength += 1
            acct_type = sa.func.pa_rep_cust_fns.fn_getIBREPType(tableObj.c.acct_id,'WEEKLYCOMMREGSTAT').label('acct_type')
            selectedColumns.append(acct_type)
        if 'is_stl' in columns:
            colLength += 1
            is_stl = sa.func.pa_rep_cust_fns.fn_isSTL(tableObj.c.acct_id).label('is_stl')
            selectedColumns.append(is_stl)
        if 'is_unreal' in columns:
            colLength += 1
            is_unreal = sa.func.pa_rep_cust_fns.fn_isUnrealAcct(tableObj.c.acct_id).label('is_unreal')
            selectedColumns.append(is_unreal)
        if 'is_margin_acct' in columns:
            colLength += 1
            is_margin_acct = sa.func.pa_rep_cust.fn.fn_isMarginAcct(tableObj.c.acct_id).label('is_margin_acct')
            selectedColumns.append(is_margin_acct)
        if 'is_ecp' in columns:
            colLength += 1
            is_ecp = sa.func.pa_rep_cust_fns.fn_isECP(tableObj.c.applicant_id).label('is_ecp')
            selectedColumns.append(is_ecp)
               
        df = pd.DataFrame(columns=['acct_id'])
        filter_ = tableObj.c.acct_id.in_(series.tolist())
    elif tableName == 'APPLICANT_RTAB':
        tableObj = sa.Table(tableName.upper(), metadata, schema='ibcust', autoload=True)
        selectedColumns = [c for c in tableObj.columns if c.name in allColumns]
        colLength = len([c for c in tableObj.columns if c.name in columns])
        df = pd.DataFrame(columns=['id'])
        filter_ = tableObj.c.id.in_(series.tolist())
        if 'acct_country' in columns:
            colLength += 1
            # pa_rep_cust_fns.getCountryLabel(upper(nvl(country_of_legal_res,country)))
            acct_country = sa.func.pa_rep_cust_fns.getCountryLabel(sa.func.upper(sa.func.nvl(tableObj.c.country_of_legal_res, tableObj.c.country))).label('acct_country')
            selectedColumns.append(acct_country)
        if 'acct_region' in columns:
            colLength += 1
            # pa_rep_cust_fns.getCountryLabel(upper(nvl(country_of_legal_res,country)))
            acct_region = sa.func.pa_rep_cust_fns.fn_getIBREPContinent(sa.func.pa_rep_cust_fns.getCountryLabel(sa.func.upper(sa.func.nvl(tableObj.c.country_of_legal_res, tableObj.c.country)))).label('acct_region')
            selectedColumns.append(acct_region)

    elif tableName == 'ACCOUNTHIERARCHY':
        tableObj = sa.Table(tableName.upper(), metadata, schema='ibcust', autoload=True)
        selectedColumns = [c for c in tableObj.columns if c.name in allColumns]
        colLength = len([c for c in tableObj.columns if c.name in columns])
        df = pd.DataFrame(columns=['sub_acct_id'])
        filter_ = tableObj.c.sub_acct_id.in_(series.tolist())
    elif tableName == 'CUSTOMERACCOUNTUSERS_RTAB':
        tableObj = sa.Table(tableName.upper(), metadata, schema='ibcust', autoload=True)
        selectedColumns = [c for c in tableObj.columns if c.name in allColumns]
        colLength = len([c for c in tableObj.columns if c.name in columns])
        df = pd.DataFrame(columns=['acct_id'])
        filter_ = tableObj.c.acct_id.in_(series.tolist())
        ibUserObj = sa.Table('IBUSER_RTAB', metadata, schema='ibcust', autoload=True)
        joinClause = tableObj.c.user_id == ibUserObj.c.id
        needsJoin = True
        moreColumns = [c for c in ibUserObj.columns if c.name in columns]
        colLength += len(moreColumns)
        selectedColumns.extend(moreColumns)
#         print(selectedColumns)
    elif tableName == 'ENTITYASSOC_RTAB':
        tableObj = sa.Table(tableName.upper(), metadata, schema='ibcust', autoload=True)
        selectedColumns = [c for c in tableObj.columns if c.name in allColumns]
        colLength = len([c for c in tableObj.columns if c.name in columns])
        df = pd.DataFrame(columns=['applicant_id'])
        filter_ = tableObj.c.applicant_id.in_(series.tolist())

        indivObj = sa.Table('INDIVIDUAL_RTAB', metadata, schema='ibcust', autoload=True)
        joinClause = tableObj.c.entity_id == indivObj.c.id
        needsJoin = True
        moreColumns = [c for c in indivObj.columns if c.name in columns]
        colLength += len(moreColumns)
        selectedColumns.extend(moreColumns)
    elif tableName == 'REP_ACCT_FIN_SUMMARY':
        tableObj = sa.Table('REP_ACCT_FIN_SUMMARY'.upper(), metadata, autoload=True)
        selectedColumns = [c for c in tableObj.columns if c.name in allColumns]
        colLength = len([c for c in tableObj.columns if c.name in columns])
        df = pd.DataFrame(columns=['acct_id'])
        maxDt = sa.func.max(tableObj.c.weekending_latest).execute().fetchone()[0]
        filter_ = (tableObj.c.acct_id.in_(series.tolist())) & (tableObj.c.weekending_latest == maxDt)
    elif tableName == 'ACCT_CASH_BAL_SUMM':
        tableObj = sa.Table(tableName.upper(), metadata, autoload=True)
        selectedColumns = [c for c in tableObj.columns if c.name in allColumns]
        colLength = len([c for c in tableObj.columns if c.name in columns])        
        df = pd.DataFrame(columns=['acct_id'])
        filter_ = tableObj.c.acct_id.in_(series.tolist())

    if tableName == 'CUSTOMERACCOUNT_RTAB':
        colLength = len(selectedColumns)

    # print(colLength)
    if colLength > 0:
        # print(tableObj.name, ':', selectedColumns)
        if needsJoin:
            selectObj = sa.select(selectedColumns, filter_).where(joinClause).execute()
        else:
            selectObj = sa.select(selectedColumns, filter_).execute()
        df = pd.DataFrame(selectObj.fetchall(), columns=selectObj.keys())

        return df
    else:
        return None

@timeit
def getAccts(engineIbcust, filterDict, asSql=False):
    '''Function for getting list of acct_ids based on a filter parsed as dictionary with syntax similar to elastic search
    usage: df = getAccts(engineIbcust, filterDict)
    ex: filterDict = {"or" : {
                    "clearing_status" :  "O",
                    "applicant_rtab.type" : "ORG"
                },
                "and" : {
                    "customeraccount_rtab.phylum_code" : {
                        "in" : ["C", "D"]
                    },
                    "rep_dim_acct.day_begun": {
                        "between": (20160801, 20160820)
                    },
                    "rep_dim_acct.acct_id": {
                        "in" : sa.text("select acct_id from ibcust.customerAccount_rtab where rownum < 6")
                    }
                }
            }
    '''
    import sqlalchemy as sa
    from pandas import DataFrame
    """ Valid operators """
    OPERATORS = {
        'like': lambda f, a: f.like(a),
        'equals': lambda f, a: f == a,
        'is_null': lambda f: f is None,
        'is_not_null': lambda f: f is not None,
        'gt': lambda f, a: f > a,
        'gte': lambda f, a: f >= a,
        'lt': lambda f, a: f < a,
        'lte': lambda f, a: f <= a,
        'in': lambda f, a: f.in_(a),
        'not_in': lambda f, a: ~f.in_(a),
        'not_equal': lambda f, a: f != a,
        'between': lambda f, a: f.between(*a),
        'acct_country': lambda f, a: sa.func.pa_rep_cust_fns.fn_getaccappcntry(f) == a
        }
    
    def verify_operator(operator):
        """ Verify if the operator is valid """
        try:
            if operator in OPERATORS:
                return True
            else:
                return False
        except KeyError:
            return False
        
    def parse_field(field, field_value):
        """ Parse the operators and traduce: ES to SQLAlchemy operators """
        if type(field_value) is dict:
            operator = list(field_value.keys())[0]
    #         print(operator, verify_operator(operator))
            if verify_operator(operator) is False:
                raise Exception("Error: operator {} does no exist".format(operator))
            value = field_value[operator]
        elif type(field_value) is str:
            operator = 'equals'
            value = field_value
        return field, operator, value
    
    def create_query(tableObj, attr):
        """ Mix all values and make the query """
        field = attr[0]
        operator = attr[1]
        value = attr[2]
        
        col = eval('tableObj.c.{}'.format(field))
        return OPERATORS[operator](col, value)
    
    # engineIbcust = getDbConnection('ORAIBCUST', asEngine=True)
    metadata = sa.MetaData(bind=engineIbcust)
    
    customeraccount_rtab = sa.Table('customeraccount_rtab'.upper(), metadata, schema='ibcust', autoload=True).alias('ca')
    applicant_rtab = sa.Table('applicant_rtab'.upper(), metadata, schema='ibcust', autoload=True).alias('ap')
    rep_dim_acct = sa.Table('rep_dim_acct'.upper(), metadata, autoload=True).alias('rda')
    
    customerAccount_cols = customeraccount_rtab.columns.keys()
    applicant_cols = applicant_rtab.columns.keys()
    rep_dim_acct_cols = rep_dim_acct.columns.keys()

    needApplicantJoin = False
    needRepDimAcctJoin = False
    q = []
    for filter_type in filterDict:
        if filter_type == 'or' or filter_type == 'and':
            conditions = []
            for field in filterDict[filter_type]:
                table_name = None
                if '.' in field:
                    fieldTmp = field.split('.')
                    table_name = fieldTmp[0]
                    field_ = fieldTmp[1]
                    if table_name == 'applicant_rtab':
                        needApplicantJoin = True
                    elif table_name == 'rep_dim_acct':
                        needRepDimAcctJoin = True
                    
                    field, operator, value = parse_field(field, filterDict[filter_type][field])
                    # print(field_, operator, value)
                    col = eval('{}.c.{}'.format(table_name, field_))
                    conditions.append(OPERATORS[operator](col, value))
                    
                    continue
                    
                attr = parse_field(field, filterDict[filter_type][field])
                
                if field in customerAccount_cols and (field in applicant_cols or field in rep_dim_acct_cols):
                    raise Exception('Column {} available in more than 1 table'.format(field))
                elif field in applicant_cols and (field in customerAccount_cols or field in rep_dim_acct_cols):
                    raise Exception('Column {} available in more than 1 table'.format(field))
                elif field in customerAccount_cols:
                    # print('Column is from {} table'.format(customeraccount_rtab))
                    # print(attr)
                    conditions.append(create_query(customeraccount_rtab, attr))
                elif field in applicant_cols:
                    # print('Column {} is from {} table'.format(field, applicant_rtab))
                    conditions.append(create_query(applicant_rtab, attr))
                    needApplicantJoin = True
                elif field in rep_dim_acct_cols:
                    conditions.append(create_query(rep_dim_acct, attr))
                    needRepDimAcctJoin = True
                else:
                    raise Exception('Column {} not available in any of the supported tables'.format(field))
            if filter_type == 'or':
                q.append(sa.or_(*conditions))
            elif filter_type == 'and':
                # print(conditions)
                q.append(sa.and_(*conditions))
        else:
            table_name = None
            if '.' in filter_type:
                fieldTmp = filter_type.split('.')
                table_name = fieldTmp[0]
                field_ = fieldTmp[1]
                if table_name == 'applicant_rtab':
                    needApplicantJoin = True
                elif table_name == 'rep_dim_acct':
                    needRepDimAcctJoin = True
                field, operator, value = parse_field(filter_type, filterDict[filter_type])
                col = eval('{}.c.{}'.format(table_name, field_))
                q.append(OPERATORS[operator](col, value))
                continue  
            attr = parse_field(filter_type, filterDict[filter_type])
            if filter_type in customerAccount_cols and (filter_type in applicant_cols or filter_type in rep_dim_acct_cols):
                raise Exception('Column {} available in more than 1 table'.format(filter_type))
            elif filter_type in applicant_cols and (filter_type in customerAccount_cols or filter_type in rep_dim_acct_cols):
                raise Exception('Column {} available in more than 1 table'.format(filter_type))
            elif filter_type in customerAccount_cols:
                q.append(create_query(customeraccount_rtab, attr))
            elif filter_type in applicant_cols:
                q.append(create_query(applicant_rtab, attr))
                needApplicantJoin = True
            elif filter_type in rep_dim_acct_cols:
                q.append(create_query(rep_dim_acct_cols, attr))
                needRepDimAcctJoin = True
                
    s = sa.select([customeraccount_rtab.c.acct_id])

    if needApplicantJoin and not needRepDimAcctJoin:
        j = customeraccount_rtab.join(applicant_rtab, customeraccount_rtab.c.applicant_id == applicant_rtab.c.id)
        s = s.select_from(j)
    if needRepDimAcctJoin and not needApplicantJoin:
        j2 = customeraccount_rtab.join(rep_dim_acct, customeraccount_rtab.c.acct_id == rep_dim_acct.c.acct_id, isouter=True)
        s = s.select_from(j2)
        
    if needApplicantJoin and needRepDimAcctJoin:
        s = s.select_from(customeraccount_rtab.join(applicant_rtab, customeraccount_rtab.c.applicant_id == applicant_rtab.c.id)
                                              .join(rep_dim_acct, customeraccount_rtab.c.acct_id == rep_dim_acct.c.acct_id, isouter=True))
    
    for c in q:
        s = s.where(c)

    if asSql:
        return str(s)
#     sys.exit()
    res = s.execute()
    df = DataFrame(res.fetchall(), columns=res.keys())
    return df