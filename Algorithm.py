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
    os.remove('B')
    os.remove('R')

def naming(number):
    name = 'B' + str(number)
    return name

def getAttributesNtypes(description):
     attributesNtypes = []
     for i in range(len(description)):
         desc = description[i]
         attributesNtypes.append((desc[0],FieldType.get_info(desc[1])))
     return attributesNtypes

def sampling(database, table_name, prim_keys, query):
    try:
        cleanup()
    except Exception as e:
        print("Couldn't cleanup: {}".format(e))

    # connect to the given database
    conn = psycopg2.connect(database=database, user="postgres", password="3526", host="127.0.0.1", port="5432")
    cursor = conn.cursor()

    # create a new database for storing repairs
    conn_repair = sqlite3.connect('R')
    cursor_repair = conn_repair.cursor()

    # create a new database for storing blocks
    conn_block = sqlite3.connect('B')
    cursor_block = conn_block.cursor()

    # get the distinct primary keys
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

    # get the number of blocks
    n = len(distinct_prims)

    # get information from the given database
    cursor.execute(sql.SQL("SELECT column_name, data_type FROM information_schema.columns WHERE table_name =  {};").format(sql.Literal(table_name)))
    attributesNtypes = cursor.fetchall()

    # get the sizes of each block
    Ms = []

    sql_str_loop = "SELECT * FROM " + table_name + " WHERE (" + ", ".join(["{}" for i in idents]) + ") = {};"
    # forming the blocks
    for i in range(0,n):
        tableName = naming(i)


        # extract all the rows for the same primary key to form a block:
        # depending on whether it's composite primary key or not
        # pdb.set_trace()
        cursor.execute(sql.SQL(sql_str_loop).format(*idents, sql.Literal(distinct_prims[i])).as_string(conn))
        pre_b = cursor.fetchall()
        pre_b = list(pre_b)
        Ms.append(len(pre_b))


        # format 'create table' statement
        create_statement = 'CREATE TABLE ' + tableName + ' ('

        for row in attributesNtypes:
            # row[0] is the attribute, row[1] is the type
            create_statement += '{} {} ,'.format(row[0], row[1])
        create_statement = create_statement[:-1] + ')'
        # create tables in database B
        cursor_block.execute(create_statement)

        # format 'insert into' statement
        insert_statement = 'INSERT INTO ' + tableName + ' VALUES '

        for row in pre_b:
            insert_statement += '{},'.format(row)
        insert_statement = insert_statement[:-1]
        cursor_block.execute(insert_statement)

    # get M for preparation of  FPRAS algorithm
    M = max(Ms)

    # random select from blocks and form repair
    repair_rows = []
    for b in range(0,n):
        tableName = naming(b)
        n_b = list(cursor_block.execute(f'''SELECT COUNT(*) FROM {tableName};'''))[0][0]
        lists = list(cursor_block.execute(f'''SELECT * FROM {tableName}'''))
        if len(lists) < 1:
            None
        else:
            repair_rows.append(random.choice(lists))

    # format 'create table' statement
    create_statement = 'CREATE TABLE ' + 'Repair' + ' ('
    # format 'insert into' statement
    insert_statement = 'INSERT INTO ' + 'Repair' + ' VALUES '

    for row in attributesNtypes:
        # row[1] is the attribute, row[2] is the type
        create_statement += '{} {} ,'.format(row[0], row[1])
    create_statement = create_statement[:-1] + ')'
    # create tables in database R
    cursor_repair.execute(create_statement)

    # print('repair_rows')
    # print(repair_rows)

    for row in repair_rows:
        insert_statement += '{},'.format(row)
    insert_statement = insert_statement[:-1]
    cursor_repair.execute(insert_statement)

    result = list(cursor_repair.execute(f'''{query}'''))

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


    # result_fpras = FPRAS('food_inspections_chicago', 'facilities', ('license_', 'aka_name'), "SELECT CASE WHEN (SELECT COUNT(*) FROM Repair WHERE (license_, aka_name) =  (2516677,'KIMCHI POP')) = 1 THEN 1 ELSE 0 END", 7, 0.68)
    result_fpras = FPRAS("lobbyists_db", "clients", ('client_id',),  "SELECT CASE WHEN (SELECT COUNT(*) FROM Repair WHERE client_id = 38662) = 1 THEN 1 ELSE 0 END", 3, 0.68)

    # print(result_sample)
    print(result_fpras)

    # cleanup()
