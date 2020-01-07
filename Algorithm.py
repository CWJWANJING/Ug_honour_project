# Authour: Wanjing Chen

import sqlite3
import os
import random
import pdb

def cleanup():
    os.remove('B')
    os.remove('R')

def naming(number):
    name = 'B' + str(number)
    return name

def sampling(database, table_name, prim_keys, query):
    try:
        cleanup()
    except Exception as e:
        print("Couldn't cleanup: {}".format(e))

    # connect to the given database
    conn = sqlite3.connect(database)
    cursor = conn.cursor()

    # create a new database for storing repairs
    conn_repair = sqlite3.connect('R')
    cursor_repair = conn_repair.cursor()

    # create a new database for storing blocks
    conn_block = sqlite3.connect('B')
    cursor_block = conn_block.cursor()

    # get the distinct primary keys
    distinct_prims = list(cursor.execute(f'''SELECT DISTINCT {prim_keys} FROM {table_name}'''))

    # get the number of blocks
    n = len(distinct_prims)

    # get information from the given database
    attributesNtypes = list(cursor.execute(f'''pragma table_info('{table_name}')'''))


    # forming the blocks
    for i in range(0,n):
        tableName = naming(i)

        # extract all the rows for the same primary key to form a block:
        # depending on whether it's composite primary key or not
        #pdb.set_trace()
        if len(prim_keys) == 1:
            pre_b = cursor.execute(f'''SELECT * FROM {table_name} WHERE ({prim_keys}) = {distinct_prims[i][0]}''')
        else:
            pre_b = cursor.execute(f'''SELECT * FROM {table_name} WHERE ({prim_keys}) = ({distinct_prims[i]})''')
        pre_b = list(pre_b)

        # format 'create table' statement
        create_statement = 'CREATE TABLE ' + tableName + ' ('

        for row in attributesNtypes:
            # row[1] is the attribute, row[2] is the type
            create_statement += '{} {} ,'.format(row[1], row[2])
        create_statement = create_statement[:-1] + ')'
        # create tables in database B
        cursor_block.execute(create_statement)

        # format 'insert into' statement
        insert_statement = 'INSERT INTO ' + tableName + ' VALUES '

        for row in pre_b:
            insert_statement += '{},'.format(row)
        insert_statement = insert_statement[:-1]
        cursor_block.execute(insert_statement)


    # random select from blocks and form repair
    repair_rows = []
    for b in range(0,n):
        tableName = naming(b)
        n_b = list(cursor_block.execute(f'''SELECT COUNT(*) FROM {tableName}'''))
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
        create_statement += '{} {} ,'.format(row[1], row[2])
    create_statement = create_statement[:-1] + ')'
    # create tables in database R
    cursor_repair.execute(create_statement)

    print('repair_rows')
    print(repair_rows)

    for row in repair_rows:
        insert_statement += '{},'.format(row)
    insert_statement = insert_statement[:-1]
    cursor_repair.execute(insert_statement)

    result = list(cursor_repair.execute(f'''{query}'''))

    if result[0][0] == 1:
        return 1
    else:
        return 0


# def FPRAS(database, query, primary_keys, epsilon, delta)

if __name__ == "__main__":
    # result = sampling('test_small.db', 'D', 'A', 'SELECT CASE WHEN (SELECT COUNT(*) FROM Repair WHERE A = 1) = 1 THEN 1 ELSE 0 END')
    # result = sampling('test_small.db', 'D', 'A, B', 'SELECT CASE WHEN (SELECT COUNT(*) FROM Repair WHERE (A,B) = (1,2)) = 1 THEN 1 ELSE 0 END')
    result = sampling('test_small.db', 'D', 'A, B', 'SELECT CASE WHEN (SELECT COUNT(*) FROM Repair WHERE (A,B,C) = (1,2,3)) = 1 THEN 1 ELSE 0 END')
    print(result)
    cleanup()
