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
    n = slist(cursor.execute(f'''SELECT COUNT (DISTINCT {prim_key}) FROM {table_name}'''))[0][0]
