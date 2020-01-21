# Authour: Wanjing Chen

import sqlite3
import os
import sys
import random
import pdb
import math
import psycopg2
from psycopg2 import sql
import decimal
import random

def cleanup():
    os.remove('RNB')

def naming(number):
    name = 'B' + str(number)
    return name

def get_prim_key_cols(prim_keys, attrs_types):
    cols = []
    for idx, row in enumerate(attrs_types):
        if row[0] in prim_keys:
            cols.append(idx)
    return cols

def get_dist_prim_keys(prim_keys, table_name, conn, cursor):
    idents = [sql.Identifier(prim) for prim in prim_keys]
    sql_str_dis = "SELECT DISTINCT " + ", ".join(["{}" for i in idents]) + " FROM " + table_name + ";"
    cursor.execute(sql.SQL(sql_str_dis).format(*idents).as_string(conn))
    pre_distinct_prims = cursor.fetchall()
    distinct_prims = []
    if len(prim_keys) == 1:
        for dis in pre_distinct_prims:
            distinct_prims.append(dis[0])
    else:
        for dis in pre_distinct_prims:
            distinct_prims.append(dis)
    return distinct_prims

def sampling(database, table_name, prim_keys, query):
    try:
        cleanup()
    except Exception as e:
        print("Couldn't cleanup: {}".format(e))

    # connect to the given database
    conn = psycopg2.connect(database=database, user="postgres", password="3526", host="127.0.0.1", port="5432")
    cursor = conn.cursor()

    # create a new database for or storing blocks and storing repairs
    conn_rnb = sqlite3.connect('RNB')
    cursor_rnb = conn_rnb.cursor()

    # get the distinct primary keys
    distinct_prims = get_dist_prim_keys(prim_keys, table_name, conn, cursor)

    # get the number of blocks
    n = len(distinct_prims)

    # get information from the given database
    # TODO: surely the below should be using Identifier not Literal?
    cursor.execute(sql.SQL("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = {};").format(sql.Literal(table_name)))
    attributesNtypes = cursor.fetchall()

    # format blocks 'create table' statement
    create_statement_b = 'CREATE TABLE Blocks ' + ' ('
    # format repairs 'create table' statement
    create_statement_r = 'CREATE TABLE Repair' + ' ('

    for row in attributesNtypes:
        # row[0] is the attribute, row[1] is the type
        create_statement_b += '{} {} ,'.format(row[0], row[1])
        create_statement_r += '{} {} ,'.format(row[0], row[1])
    create_statement_b += 'B text)'
    create_statement_r = create_statement_r[:-1] + ')'
    # create tables in database
    cursor_rnb.execute(create_statement_b)
    cursor_rnb.execute(create_statement_r)


    # get the sizes of each block
    Ms = []

    sql_str_loop = "SELECT * FROM " + table_name + " ORDER BY " + ", ".join(prim_keys) + ";"
    # forming the blocks
    cursor.execute(sql_str_loop)
    table = cursor.fetchall()

    cols = get_prim_key_cols(prim_keys, attributesNtypes)

    prev_prim_keys = None
    max_m = 0
    m = 0
    insert_statement = "INSERT INTO Blocks VALUES "
    count_block = -1
    current_block = []
    repair_rows = []
    # pdb.set_trace()
    for idx, row in enumerate(table):
        new_prim_key = [row[col] for col in cols]

        if idx == 0 or new_prim_key == prev_prim_keys:
            m += 1
            current_block.append(row)
        else:
            # print(current_block)
            # print(m)
            # print('idx')
            random_idx = random.randint(0,m-1)
            # print(random_idx)
            repair_rows.append(current_block[random_idx])
            # print('repair_rows', repair_rows)
            current_block = []
            current_block.append(row)
            # if in new block, finish prev block m
            if m > max_m:
                max_m = m
            # now in new block, so seen 1 row so far
            m = 1
            count_block += 1
        block_no = naming(count_block)

        # insert_statement += str((*row, block_no)) + ", "
        prev_prim_keys = new_prim_key

    # cursor_rnb.execute(insert_statement[:-2])
    print(f"Inserted {idx+1} rows, {count_block+1} blocks")

    # get M for preparation of  FPRAS algorithm
    M = max_m

    # format 'insert into' statement
    insert_statement = 'INSERT INTO Repair VALUES '
    for row in repair_rows:
        # row = row[:-1]
        insert_statement += '{},'.format(row)
    insert_statement = insert_statement[:-1]
    cursor_rnb.execute(insert_statement)

    result = list(cursor_rnb.execute(f'''{query}'''))

    if result[0][0] == 1:
        return (1,M)
    else:
        return (0,M)


def FPRAS(database, table_name,  prim_keys, query, epsilon, delta):
    # connect to the given database
    conn = psycopg2.connect(database=database, user="postgres", password="3526", host="127.0.0.1", port="5432")
    cursor = conn.cursor()

    # get all attributes
    cursor.execute(sql.SQL("SELECT column_name FROM information_schema.columns WHERE table_name =  {};").format(sql.Literal(table_name)))
    pre_attributes = cursor.fetchall()
    attributes = []
    for att in pre_attributes:
        attributes.append(att[0])

    # get maximum size of the blocks
    M = sampling(database, table_name, prim_keys, query)[1]
    print('M (the maximum size of the blocks): ', M)

    # get keywidth
    k = 0
    for a in attributes:
        if a in query:
            k += 1
    print('k (keywidth): ', k)

    mathLog = math.log(2/delta)
    N = ((decimal.Decimal((2+epsilon))*pow(M,k))/pow(epsilon,2))*decimal.Decimal(mathLog)
    print('N: ', N)
    NLoop = math.ceil(N)

    count = 0
    for i in range(0, NLoop):
        count += sampling(database, table_name, prim_keys, query)[0]
    print('The sum of sampling score is: ', count)
    print('The probability is: ')
    return count/N

if __name__ == "__main__":
    # result_sample = sampling('test_small.db', 'D', 'A', 'SELECT CASE WHEN (SELECT COUNT(*) FROM Repair WHERE A = 1) = 1 THEN 1 ELSE 0 END')
    # result_sample = sampling('test_small.db', 'D', 'A, B', 'SELECT CASE WHEN (SELECT COUNT(*) FROM Repair WHERE (A,B) = (1,2)) = 1 THEN 1 ELSE 0 END')

    # result_sample = sampling('food_inspections_chicago', 'facilities', ('license_', 'aka_name'), "SELECT CASE WHEN (SELECT COUNT(*) FROM Repair WHERE (license_, aka_name) =  (2516677,'KIMCHI POP')) = 1 THEN 1 ELSE 0 END")[0]
    # result_sample = sampling("lobbyists_db", "clients", ('client_id',), "SELECT CASE WHEN (SELECT COUNT(*) FROM Repair WHERE client_id = 38662) = 1 THEN 1 ELSE 0 END")[0]
    # result_sample = sampling("traffic_crashes_chicago", "locations", ('street_name', 'street_no', 'street_direction'),  "SELECT CASE WHEN (SELECT COUNT(*) FROM Repair WHERE (street_name, street_no, street_direction) = ('ARCHER AVE', '3652', 'S')) = 1 THEN 1 ELSE 0 END")[0]

    # result_fpras = FPRAS('food_inspections_chicago', 'facilities', ('license_', 'aka_name'), "SELECT CASE WHEN (SELECT COUNT(*) FROM Repair WHERE (license_, aka_name) =  (2516677,'KIMCHI POP')) = 1 THEN 1 ELSE 0 END", 7, 0.68)
    # result_fpras = FPRAS("lobbyists_db", "clients", ('client_id',),  "SELECT CASE WHEN (SELECT COUNT(*) FROM Repair WHERE client_id = 38662) = 1 THEN 1 ELSE 0 END", 3, 0.68)
    result_fpras = FPRAS("traffic_crashes_chicago", "locations", ('street_name', 'street_no', 'street_direction'),  "SELECT CASE WHEN (SELECT COUNT(*) FROM Repair WHERE (street_name, street_no, street_direction) = ('ARCHER AVE', '3652', 'S')) = 1 THEN 1 ELSE 0 END", 3, 0.88)

    # print(result_sample)
    print(result_fpras)

    # cleanup()
