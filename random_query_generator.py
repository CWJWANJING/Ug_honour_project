# random query backup
import psycopg2
from psycopg2 import sql
import random
from collections import defaultdict
import numpy as np

def fast_randint(size):
    """Returns a random integer between 0 and size
    faster than random.randint
    From:
    https://eli.thegreenplace.net/2018/slow-and-fast-methods-for-generating-random-integers-in-python/
    """
    return int(random.random()*size)

def random_query(database, primary_keys_multi):
    conn = psycopg2.connect(database=database, user="postgres",
        password="230360", host="127.0.0.1", port="5432")
    cursor = conn.cursor()
    # get the table names
    cursor.execute(
        sql.SQL(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_type='BASE TABLE' AND table_schema='public';"
        ))
    Names = cursor.fetchall()
    tableNames = []
    for n in Names:
        tableNames.append(n[0])
    # get the attributes and their types
    dict_attributesNtypes = defaultdict(int)
    for n in tableNames:
        cursor.execute(
            sql.SQL(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_name =  {} and table_schema = 'public';").format(sql.Literal(n))
            )
        attributesNtypes = cursor.fetchall()
        dict_attributesNtypes[n] = attributesNtypes
    # extract the attributes from dict_attributesNtypes,
    # so that we don't need to use fetchall again, since it takes lots of time
    dict_tables = defaultdict(int)
    dict_attributes = defaultdict(int)
    columns = []
    for i in range(len(tableNames)):
        each_columns = [attributesNtypes[0] for attributesNtypes in dict_attributesNtypes[tableNames[i]]]
        columns.append(each_columns)
        dict_attributes[tableNames[i]] = each_columns
        # get all the table data, ordering by primary keys for forming blocks
        sql_str_loop =  f"SELECT * FROM {tableNames[i]} " \
                        f"ORDER BY {', '.join(primary_keys_multi[i])};"

        cursor.execute(sql_str_loop)
        table = cursor.fetchall()
        dict_tables[tableNames[i]] = table
    # flatten columns to become single list of values
    # flat_columns = [item for sublist in columns for item in sublist]

    # put values for each attribute into corresponding dictionary
    # use nested dictionary in this case
    dict_tables_columns = defaultdict(int)
    for i in range(len(columns)):
        current_columns = columns[i]
        dict_columns = defaultdict(int)
        for m in range(len(columns[i])):
            dict_columns[current_columns[m]] = [row_values[m] for row_values in dict_tables[tableNames[i]]]
        dict_tables_columns[tableNames[i]] = dict_columns
    # now start query information
    # get dict which stores tables have same attributes, prepare for join
    dict_tables_join = defaultdict(int)
    dict_columns_join = defaultdict(int)
    for i in range(len(tableNames)-1):
        a = dict_attributes[tableNames[i]]
        b = dict_attributes[tableNames[i+1]]
        common = list(set(a) & set(b))
        if common != None:
            dict_tables_join[tableNames[i]] =
    # random select from 'inner join', 'left join', 'right join', 'full join', 'comma join'
    opt_pre = ['inner join', 'left join', 'right join', 'full join', 'comma join']
    n_opt_pre = fast_randint(5)
    if n_opt_pre == 4:
        # select random number of tables in the database
        n_tables = fast_randint(len(tableNames))
        random_index_tables = np.unique(np.random.randint(0, 5, size=n_tables))
        random_tables = [tableNames[i] for i in random_index_tables]
        query_from = f" from {', '.join(random_tables)} "
    else:

    # select randomly from 'all', 'distinct', 'random columns'
    opt_1 = ['*', 'distinct', 'random_columns']
    n_opt_1 = fast_randint(3)
    if n_opt_1 == 2:
        columns_select = []
        for t in random_tables:
            # doesn't consider cases when attribute names in tables collapse
            n_columns = fast_randint(len(dict_attributes[t]))
            columns_select.append(dict_attributes[t][n_columns])
        columns_select = ', '.join(columns_select)
        query_select = f"select {columns_select} "
    else:
        query_select = f"select {opt_1[n_opt_1]} "
    query = query_select + query_from
    print(query)
    # random filtering based on real values


    return None

if __name__ == "__main__":
    print(random_query("lobbyists_db", [('client_id',),('compensation_id',),('contribution_id',),('employer_id',),('gift_id',),('lobbying_activity_id',),('lobbyist_id',)]))
