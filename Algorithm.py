# Authour: Wanjing Chen

import csv
import sys
import sqlite3
import os
import os.path
import random
import pdb
import math
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import decimal
from decimal import Decimal
import datetime
import time
import numpy as np
from random_query_generator import *

def fast_randint(size):
    """Returns a random integer between 0 and size
    faster than random.randint
    From:
    https://eli.thegreenplace.net/2018/slow-and-fast-methods-for-generating-random-integers-in-python/
    """
    return int(random.random()*size)

def cleanup():
    conn = psycopg2.connect(dbname="postgres",
      user="postgres", host="127.0.0.1",
      password='230360', port="5432")

    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    cursor.execute(sql.SQL("SELECT pg_terminate_backend (pg_stat_activity.pid)"
                            "FROM pg_stat_activity WHERE pg_stat_activity.datname = 'RNB';"))
    try:
        cursor.execute(sql.SQL("DROP DATABASE \"RNB\""))
    except psycopg2.errors.InvalidCatalogName as e:
        print(f"Couldn't clean up RNB: {e}")

# get the indexes of primary keys
def get_prim_key_cols(prim_keys, attributesNtypes):
    cols = []
    for idx, row in enumerate(attributesNtypes):
        if row[0] in prim_keys:
            cols.append(idx)
    return cols

def create_repairs_blocks_table(attributesNtypes, table_name, cursor_rnb):
    # format repairs 'create table' statement
    create_statement_r = 'CREATE TABLE ' + table_name + ' ('

    # row[0] is the attribute, row[1] is the type
    for row in attributesNtypes:
        create_statement_r += '{} {}, '.format(row[0], row[1])
    create_statement_r = create_statement_r[:-2] + ')'
    cursor_rnb.execute(create_statement_r)

# this function was for those tpc databases
# def toStr(row):
#     new_row = []
#     for r in row:
#         r = str(r)
#         new_row.append(r)
#     new_row = str(new_row).strip('[]')
#     return new_row

# def insert_repair_table_copy(repair_rows, table_name, cursor_rnb):
#     with open('tmp.copy', 'w') as copyfile:
#         w = csv.writer(copyfile, delimiter='\t')
#         w.writerows(repair_rows)
#     with open('tmp.copy', 'r') as copyfile:
#         cursor_rnb.copy_from(copyfile, table_name)
#     cursor_rnb.execute('''SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema='public';''')
#     r = cursor_rnb.fetchall()
#     for i in r:
#         print(i[0])
#         print(cursor_rnb.execute(f'''select * from {i[0]} limit 10;'''))
#     print(r)
#
#     os.remove('tmp.copy')

# def format_v(t):
#     temp_t = list(t)
#     for idx, s in enumerate(temp_t):
#         if '\'' in s:
#             temp_t[idx] = "$$" + s + "$$"
#     return tuple(temp_t)

def insert_repair_table(repair_rows, table_name, cursor_rnb, dict_attributesNtypes, conn_rnb):
    # format 'insert into' statement

    insert_statement = 'INSERT INTO ' + table_name + "("
    for at in dict_attributesNtypes:
        insert_statement += " " + at[0] + ","

    insert_statement = insert_statement[:-1] + ")" + " VALUES "
    ini_insert_statement = insert_statement

    # ini_insert_statement = insert_statement

    l = len(repair_rows[0])

    # print(repair_rows)
    count = 0
    for row in repair_rows:
        count += 1
        for i in range(l):
            if i == 0:
                insert_statement += "("
                insert_statement += "$${}$$,".format(row[i])
            elif i == (l-1):
                insert_statement += "$${}$$,".format(row[i])
                insert_statement = insert_statement[:-1] + "),"
            else:
                insert_statement += "$${}$$,".format(row[i])
        # print(insert_statement)
        # print(count)
        if count<1000 and count == len(repair_rows):
            insert_statement = insert_statement[:-1]
            # print(insert_statement)
            cursor_rnb.execute(insert_statement)
        elif count>0 and count%1000 == 0:
            insert_statement = insert_statement[:-1]
            # print("1000 statement" + insert_statement + "\n \n\n\n\n\n\n\n")
            cursor_rnb.execute(insert_statement)
            insert_statement = ini_insert_statement
        else:
            if (len(repair_rows)-count) == len(repair_rows)%1000:
                insert_statement = insert_statement[:-1]
                cursor_rnb.execute(insert_statement)

    conn_rnb.commit()
    return cursor_rnb
        # insert_statement = insert_statement[:-1] + ")"
        # print(insert_statement)


        # row = toStr(row)
        # hehe = "i'm good"
        # cursor_rnb.execute(f"INSERT INTO {table_name} VALUES ($${row[0]}$$), ($${row[1]}$$)")
        # insert_statement = ini_insert_statement

    # insert_values = ""
    # for row in repair_rows:
    #     row = toStr(row)
    #     count += 1
    #     row = format_v(row)
    #     insert_values += "{},".format(row)
    #
    #     if count>0 and count%1000 == 0:
    #         insert_values = insert_values[:-1]
    #         # print("1000 statement" + insert_statement)
    #         cursor_rnb.execute(insert_statement, (insert_values, ))
    #         insert_values = ""
    #     else:
    #         if (len(repair_rows)-count) == len(repair_rows)%1000:
    #             insert_values = insert_values[:-1]
    #             cursor_rnb.execute(insert_statement, (insert_values, ))

# for loop forms the blocks and in the mean time,
# do random selection and forms the repair rows
def blocks_repairs_formation(table, cols):
    prev_prim_keys = None
    max_m = 0
    m = 0
    count_block = -1
    current_block = []
    repair_rows = []

    for idx, row in enumerate(table):
        new_prim_key = [row[col] for col in cols]
        if idx == 0 or new_prim_key == prev_prim_keys:
            m += 1
            current_block.append(row)
        else:
            # this commented code is for finding the violated primary keys
            # in the database
            # if m > 1:
            #     print(current_block)
            random_idx = fast_randint(m-1)
            repair_rows.append(current_block[random_idx])
            current_block = []
            current_block.append(row)
            # if in new block, finish prev block m
            if m > max_m:
                max_m = m
            # now in new block, so seen 1 row so far
            m = 1
            count_block += 1
        prev_prim_keys = new_prim_key
    # print(f"Inserted {idx+1} rows, {count_block+2} blocks")
    return max_m, repair_rows

def create_and_switch_to_rnb():
    conn_rnb = psycopg2.connect(dbname="postgres",
      user="postgres", host="127.0.0.1",
      password='230360', port="5432")

    conn_rnb.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor_rnb = conn_rnb.cursor()
    cursor_rnb.execute(sql.SQL("CREATE DATABASE \"RNB\""))

    conn_rnb.close()
    conn_rnb = psycopg2.connect(dbname="RNB",
      user="postgres", host="127.0.0.1",
      password='230360', port="5432")

    return conn_rnb

def pre_sampling(database, primary_keys_multi):

    dict_tables, dict_attributesNtypes, tables_filter, dict_attributes, query, tuple = random_query(
        database, primary_keys_multi)

    conn_rnb = create_and_switch_to_rnb()
    cursor_rnb = conn_rnb.cursor()

    for t in tables_filter:
        attributesNtypes = dict_attributesNtypes[t]
        create_repairs_blocks_table(attributesNtypes, t, cursor_rnb)

    return dict_attributesNtypes, dict_tables, conn_rnb, dict_attributes, tables_filter, query, tuple

def check_in(tuple, result):
    for r in result:
        boo =  all(elem in r for elem in tuple)
        if boo == True:
            return 1
        else:
            continue
    return 0

def sampling_loop(dict_tables, dict_attributesNtypes, primary_keys_multi, query, tuple, tableNames, conn_rnb):
    cursor_rnb = conn_rnb.cursor()
    Ms = []
    # tic = time.perf_counter()
    for i in range(len(primary_keys_multi)):
        cols = get_prim_key_cols(primary_keys_multi[i], dict_attributesNtypes[tableNames[i]])
        M, repair_rows = blocks_repairs_formation(dict_tables[tableNames[i]], cols)
        cursor_rnb = insert_repair_table(repair_rows, tableNames[i], cursor_rnb, dict_attributesNtypes[tableNames[i]], conn_rnb)
        Ms.append(M)

    cursor_rnb.execute(query)
    result = cursor_rnb.fetchall()
    score = check_in(tuple, result)
    M = max(Ms)
    # toc = time.perf_counter()
    # print(f"Sampling ran in {toc - tic:0.4f} seconds")

    for n in tableNames:
        cursor_rnb.execute(f'''drop table if exists {n}''')
        create_repairs_blocks_table(dict_attributesNtypes[n], n, cursor_rnb)

    return score, M

def FPRAS(database, primary_keys_multi, dict_primary_keys, epsilon, delta):
    tic = time.perf_counter()
    dict_attributesNtypes, dict_tables, conn_rnb, dict_attributes, tables_filter, query, tuple = pre_sampling(database, primary_keys_multi)
    print(tuple)
    print(query)
    toc = time.perf_counter()
    print((f"Pre_sampling ran in {toc - tic:0.4f} seconds"))
    # initialise keywidth
    k = 0
    primary_keys_multi = []
    for n in tables_filter:
        primary_keys_multi.append(dict_primary_keys[n])
        attributes = dict_attributes[n]
        for a in attributes:
            if a in query:
                k += 1
    print('k (keywidth): ', k)
    # get maximum size of the blocks
    M = sampling_loop(dict_tables, dict_attributesNtypes, primary_keys_multi, query, tuple, tables_filter, conn_rnb)[1]
    print('M (the maximum size of the blocks): ', M)

    mathLog = math.log(2/delta)
    N = ((decimal.Decimal((2+epsilon))*pow(M,k))/pow(decimal.Decimal(epsilon),2))*decimal.Decimal(mathLog)
    print('N: ', N)
    NLoop = math.ceil(N)

    count = 0
    index = 0
    for i in range(0, NLoop):
        count += sampling_loop(dict_tables, dict_attributesNtypes, primary_keys_multi, query, tuple, tables_filter, conn_rnb)[0]
        index+=1
        print("Index: ", index)
        print("Score: ", count)
        # sys.exit()
    print('The sum of sampling score is: ', count)
    print('The probability is: ', count/NLoop)
    return count/NLoop


if __name__ == "__main__":
    # result_sample = sampling('test_small.db', 'D', 'A', 'SELECT CASE WHEN (SELECT COUNT(*) FROM Repair WHERE A = 1) = 1 THEN 1 ELSE 0 END')
    # result_sample = sampling('test_small.db', 'D', 'A, B', 'SELECT CASE WHEN (SELECT COUNT(*) FROM Repair WHERE (A,B) = (1,2)) = 1 THEN 1 ELSE 0 END')
    # results = []
    # for i in range(0,1):
        # result_sample = sampling('food_inspections_chicago', 'facilities', ('license_', 'aka_name'), "SELECT CASE WHEN (SELECT COUNT(*) FROM Repair WHERE (license_, aka_name) =  (1299537, 'GALLERIA MARKET')) = 1 THEN 1 ELSE 0 END")[0]

    cleanup()
    primary_keys_multi = [('client_id',),('compensation_id',),('contribution_id',),('employer_id',),('gift_id',),('lobbying_activity_id',),('lobbyist_id',)]
    dict_primary_keys = {
            "clients": 'client_id',
            "compensations" : 'compensation_id' ,
            "contributions" : 'contribution_id',
            "employers" : 'employer_id',
            "gifts" : 'gift_id',
            "lobbying_activities" : 'lobbying_activity_id',
            "lobbyists" : 'lobbyist_id'
            }
    tic = time.perf_counter()
    result_fpras = FPRAS("lobbyists_db", primary_keys_multi, dict_primary_keys, 0.1, 0.75)
    toc = time.perf_counter()
    print(f"FPRAS ran in {toc - tic:0.4f} seconds")
    # print(format_v(("202020-3030-2939", "O'hi"))


        # result_sample = sampling("traffic_crashes_chicago", "locations", ('street_name', 'street_no', 'street_direction'),  "SELECT CASE WHEN (SELECT COUNT(*) FROM Repair WHERE (street_name, street_no, street_direction) = ('ARCHER AVE', '3652', 'S')) = 1 THEN 1 ELSE 0 END")[0]
        # result_fpras = FPRAS('food_inspections_chicago', 'facilities', ('license_', 'aka_name'), "SELECT CASE WHEN (SELECT COUNT(*) FROM Repair WHERE (license_, aka_name) =  (2516677,'KIMCHI POP')) = 1 THEN 1 ELSE 0 END", 0.6, 0.5)
        # results.append(result_sample)
        # result_fpras = FPRAS("lobbyists_db", "clients", ('client_id',),  "SELECT CASE WHEN (SELECT COUNT(*) FROM Repair WHERE client_id = 38662) = 1 THEN 1 ELSE 0 END", 0.6, 0.4)
        # result_fpras = FPRAS("traffic_crashes_chicago", "locations", ('street_name', 'street_no', 'street_direction'),  "SELECT CASE WHEN (SELECT COUNT(*) FROM Repair WHERE (street_name, street_no, street_direction) = ('ARCHER AVE', '3652', 'S')) = 1 THEN 1 ELSE 0 END", 0.7, 0.88)
    # result_fpras = FPRAS('out1_2', ('public_experiment_q1_1_30_2_5.lineitem', 'public_experiment_q1_1_30_2_5.partsupp'), [('l_orderkey', 'l_linenumber'), ('ps_partkey', 'ps_suppkey')],"SELECT CASE WHEN (SELECT COUNT(*) FROM lineitem, partsupp WHERE lineitem.l_suppkey = partsupp.ps_suppkey AND partsupp.ps_availqty = 674 AND lineitem.l_tax = 0.000) = 1 THEN 1 ELSE 0 END", 0.1, 0.75 )
    # result_test_loop = test_loop('out1_2', ('public_experiment_q1_1_30_2_5.lineitem', 'public_experiment_q1_1_30_2_5.partsupp'), [('l_orderkey', 'l_linenumber'), ('ps_partkey', 'ps_suppkey')],"SELECT CASE WHEN (SELECT COUNT(*) FROM lineitem, partsupp WHERE lineitem.l_suppkey = partsupp.ps_suppkey AND partsupp.ps_availqty = 674 AND lineitem.l_tax = 0.000) = 1 THEN 1 ELSE 0 END" )

        # "SELECT CASE WHEN (SELECT COUNT(*) FROM lineitem, partsupp WHERE lineitem.l_suppkey = partsupp.ps_suppkey AND partsupp.ps_availqty = 674 AND lineitem.l_tax = 0.000) = 1 THEN 1 ELSE 0 END",
        # 0.6, 0.7)

    # print(result_fpras)
        # print('Experiment: ', i)
    # print(results.count(1))

    # print(toStr((998, 10143, 5146, 4, Decimal('6.00'), Decimal('6318.84'), Decimal('0.09'), Decimal('0.05'), 'R', 'F', datetime.date(1995, 3, 20), datetime.date(1994, 12, 27), datetime.date(1995, 4, 13), 'DELIVER IN PERSON        ', 'MAIL      ', 'refully accounts. carefully express ac')))
        # result_sample = sampling('out1_2', ('public_experiment_q1_1_30_2_5.lineitem', 'public_experiment_q1_1_30_2_5.partsupp'), [('l_orderkey', 'l_linenumber'), ('ps_partkey', 'ps_suppkey')], "SELECT CASE WHEN (SELECT COUNT(*) FROM lineitem, partsupp WHERE lineitem.l_suppkey = partsupp.ps_suppkey AND partsupp.ps_availqty = 674 AND lineitem.l_tax = 0.000) = 1 THEN 1 ELSE 0 END" )

    # print(result_test_loop)
    # print(result_sample)
    # print(result_fpras)
