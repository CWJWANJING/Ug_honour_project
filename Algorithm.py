# Authour: Wanjing Chen

import sqlite3



def sampling(database, table_name, prim_key, query):
    # connect to the given database
    conn = sqlite3.connect(database)
    cursor = conn.cursor()

    # create a new database for storing repairs
    conn_repair = sqlite3.connect('R')
    cursor_repair = conn_repair.cursor()

    # create a new database for storing blocks
    conn_block = sqlite3.connect('B')
    cursor_block = conn_block.cursor()

    # get the number of blocks first
    n = list(cursor.execute(f'''SELECT COUNT (DISTINCT {prim_key}) FROM {table_name}'''))[0][0]

    # get the vioated primary keys
    violate_prims = list(cursor.execute(f'''SELECT COUNT (DISTINCT {prim_key}) FROM {table_name}'''))

    for i in range(0,n-1):
        # extract all the rows for the same primary key to form a block
        pre_b = cursor.execute(f'''SELECT * FROM {table_name} WHERE {prim_key} = {violate_prims[i][0]}''')
