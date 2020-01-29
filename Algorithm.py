# Authour: Wanjing Chen

import sqlite3
import os
import random
import pdb
import math
import psycopg2
from psycopg2 import sql
import decimal

def cleanup():
    os.remove('RNB')

def naming(number):
    name = 'B' + str(number)
    return name

def get_prim_key_cols(prim_keys, attributesNtypes):    # get the location of primary keys
    cols = []
    for idx, row in enumerate(attributesNtypes):
        if row[0] in prim_keys:
            cols.append(idx)
    return cols

def get_dist_prim_keys(prim_keys, table_name, conn, cursor):    # get the distinct primary key values
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

def create_repairs_blocks_table(attributesNtypes, cursor_rnb):
    create_statement_b = 'CREATE TABLE Blocks ' + ' ('   # format blocks 'create table' statement
    create_statement_r = 'CREATE TABLE Repair' + ' ('    # format repairs 'create table' statement

    for row in attributesNtypes:    # row[0] is the attribute, row[1] is the type
        create_statement_r += '{} {} ,'.format(row[0], row[1])
    create_statement_r = create_statement_r[:-1] + ')'
    cursor_rnb.execute(create_statement_r)

def insert_repair_table(repair_rows, cursor_rnb):
    insert_statement = 'INSERT INTO Repair VALUES '    # format 'insert into' statement
    for row in repair_rows:
        insert_statement += '{},'.format(row)
    insert_statement = insert_statement[:-1]
    cursor_rnb.execute(insert_statement)

def blocks_repairs_formation(table, cols):    # for loop forms the blocks and in the mean time, do random selection and forms the repair rows
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
            random_idx = random.randint(0,m-1)
            repair_rows.append(current_block[random_idx])
            current_block = []
            current_block.append(row)
            if m > max_m:    # if in new block, finish prev block m
                max_m = m
            m = 1    # now in new block, so seen 1 row so far
            count_block += 1
        block_no = naming(count_block)
        prev_prim_keys = new_prim_key
    print(f"Inserted {idx+1} rows, {count_block+1} blocks")
    return max_m, repair_rows

def sampling(database, table_name, prim_keys, query):
    try:
        cleanup()
    except Exception as e:
        print("Couldn't cleanup: {}".format(e))

    conn = psycopg2.connect(database=database, user="postgres", password="3526", host="127.0.0.1", port="5432")    # connect to the given database
    cursor = conn.cursor()

    conn_rnb = sqlite3.connect('RNB')    # create a new database for or storing blocks and storing repairs
    cursor_rnb = conn_rnb.cursor()

    distinct_prims = get_dist_prim_keys(prim_keys, table_name, conn, cursor)    # get the distinct primary keys

    cursor.execute(sql.SQL("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = {};").format(sql.Literal(table_name)))    # get information from the given database
    attributesNtypes = cursor.fetchall()

    create_repairs_blocks_table(attributesNtypes, cursor_rnb)

    sql_str_loop = "SELECT * FROM " + table_name + " ORDER BY " + ", ".join(prim_keys) + ";"    # get all the table data, ordering by primary keys for forming blocks
    cursor.execute(sql_str_loop)
    table = cursor.fetchall()

    cols = get_prim_key_cols(prim_keys, attributesNtypes)
    M, repair_rows = blocks_repairs_formation(table, cols)
    insert_repair_table(repair_rows, cursor_rnb)

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

    result_sample = sampling('food_inspections_chicago', 'facilities', ('license_', 'aka_name'), "SELECT CASE WHEN (SELECT COUNT(*) FROM Repair WHERE (license_, aka_name) =  (2516677,'KIMCHI POP')) = 1 THEN 1 ELSE 0 END")[0]
    # result_sample = sampling("lobbyists_db", "clients", ('client_id',), "SELECT CASE WHEN (SELECT COUNT(*) FROM Repair WHERE client_id = 38662) = 1 THEN 1 ELSE 0 END")[0]
    # result_sample = sampling("traffic_crashes_chicago", "locations", ('street_name', 'street_no', 'street_direction'),  "SELECT CASE WHEN (SELECT COUNT(*) FROM Repair WHERE (street_name, street_no, street_direction) = ('ARCHER AVE', '3652', 'S')) = 1 THEN 1 ELSE 0 END")[0]

    # result_fpras = FPRAS('food_inspections_chicago', 'facilities', ('license_', 'aka_name'), "SELECT CASE WHEN (SELECT COUNT(*) FROM Repair WHERE (license_, aka_name) =  (2516677,'KIMCHI POP')) = 1 THEN 1 ELSE 0 END", 7, 0.68)
    # result_fpras = FPRAS("lobbyists_db", "clients", ('client_id',),  "SELECT CASE WHEN (SELECT COUNT(*) FROM Repair WHERE client_id = 38662) = 1 THEN 1 ELSE 0 END", 3, 0.68)
    # result_fpras = FPRAS("traffic_crashes_chicago", "locations", ('street_name', 'street_no', 'street_direction'),  "SELECT CASE WHEN (SELECT COUNT(*) FROM Repair WHERE (street_name, street_no, street_direction) = ('ARCHER AVE', '3652', 'S')) = 1 THEN 1 ELSE 0 END", 3, 0.88)

    print(result_sample)
    # print(result_fpras)

    # cleanup()
