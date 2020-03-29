# random query backup
import psycopg2
from psycopg2 import sql
import random
import numpy as np
import pdb

def fast_randint(size):
    """Returns a random integer between 0 and size
    faster than random.randint
    From:
    https://eli.thegreenplace.net/2018/slow-and-fast-methods-for-generating-random-integers-in-python/
    """
    return int(random.random()*size)

def getAttributesNtypes(database):
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
    dict_attributesNtypes = {}
    for n in tableNames:
        cursor.execute(
            sql.SQL(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_name =  {} and table_schema = 'public';").format(sql.Literal(n))
            )
        attributesNtypes = cursor.fetchall()
        dict_attributesNtypes[n] = attributesNtypes
    return cursor, tableNames, dict_attributesNtypes

def dictionariesFormation(tableNames, dict_attributesNtypes, primary_keys_multi, cursor):
    dict_tables = {}
    dict_attributes = {}
    dict_primary_keys = {}
    columns = []
    for i in range(len(tableNames)):
        dict_primary_keys[tableNames[i]] = primary_keys_multi[i]
        each_columns = [attributesNtypes[0] for attributesNtypes in dict_attributesNtypes[tableNames[i]]]
        columns.append(each_columns)
        dict_attributes[tableNames[i]] = each_columns
        # get all the table data, ordering by primary keys for forming blocks
        sql_str_loop =  f"SELECT * FROM {tableNames[i]} " \
                        f"ORDER BY {', '.join(primary_keys_multi[i])};"
        cursor.execute(sql_str_loop)
        table = cursor.fetchall()
        dict_tables[tableNames[i]] = table
    # put values for each attribute into corresponding dictionary
    # use nested dictionary in this case
    dict_tables_columns = {}
    for i in range(len(columns)):
        current_columns = columns[i]
        dict_columns = {}
        for m in range(len(columns[i])):
            dict_columns[current_columns[m]] = [row_values[m] for row_values in dict_tables[tableNames[i]]]
        dict_tables_columns[tableNames[i]] = dict_columns
    return dict_tables_columns, dict_attributes, dict_tables, dict_primary_keys

def joinPreparation(tableNames, dict_attributes):
    # get dict which stores tables have same attributes, prepare for join
    dict_tables_join = {}
    for table_i in tableNames:
        dict_columns_join = {}
        a = dict_attributes[table_i]
        for table_n in tableNames:
            if table_n != table_i:
                b = dict_attributes[table_n]
                common = list(set(a) & set(b))
                if common != []:
                    # pdb.set_trace()
                    dict_columns_join[table_n] = common
                    dict_tables_join[table_i] = dict_columns_join
    return dict_tables_join

def joinFormation(tableNames, dict_tables_join):
    # random select from 'inner join', 'left join', 'right join', 'full join'
    opt_pre = ['inner join', 'left join', 'right join', 'full join']
    n_opt_pre = fast_randint(4)
    # +1 because we need at least one table to be selected
    n_tables = fast_randint(len(tableNames))+1
    tables_filter = []
    j_attributes = []
    query_from = " from"

    table1 = random.choice(tableNames)
    random_join_index = fast_randint(len(dict_tables_join[table1]))
    join_type = random.choice(opt_pre[:-1])
    tables_2 = list(dict_tables_join[table1].keys())
    table2 = tables_2[random_join_index]
    join_attributes = dict_tables_join[table1][table2]
    j_att = random.choice(join_attributes)
    query_from = f" from {table1} t1 {join_type} {table2} t2 on t1.{j_att} = t2.{j_att}"
    tables_filter.append(table1)
    tables_filter.append(table2)
    j_attributes = join_attributes
    return tables_filter, query_from, j_attributes

def check_union(union, j_attributes):
    new_u = []
    for s in union:
        if s in j_attributes:
            s = "t1." + s
            new_u.append(s)
        else:
            new_u.append(s)
    return new_u

def selectFormation(tables_filter, dict_attributes, j_attributes, dict_primary_keys):
    # select randomly from ''distinct', 'random columns'
    n_opt_2 = fast_randint(2)
    dict_vio_select = {}
    for t in tables_filter:
        n_columns = fast_randint(len(dict_attributes[t])-1)+1
        dict_vio_select[t] = dict_attributes[t][n_columns]
    n_prim = fast_randint(len(tables_filter))
    tuple_table = tables_filter[n_prim]
    columns_prims = dict_primary_keys[tuple_table]
    list2 = []
    list2.append(dict_vio_select[tuple_table])
    tuple_attributes = list(set(columns_prims) | set(list2))
    # if joint tables have common keys
    if j_attributes != []:
        union = list(set(tuple_attributes) | set(j_attributes))
        union = check_union(union, j_attributes)
        union = ", ".join(union)
        if n_opt_2 == 0:
            query_select = f"select {union}"
        else:
            query_select = f"select distinct {union}"
    # if joint tables don't have common keys
    else:
        if n_opt_2 == 0:
            query_select = f"select {tuple_attributes}"
        else:
            query_select = f"select distinct {tuple_attributes}"
    return query_select, tuple_attributes, tuple_table

def filterFormation(tables_filter, dict_tables_columns):
    # randomly filter the values
    # randomly decide to filter how many set of values
    filter_times = fast_randint(2)
    query_where = " where"
    if filter_times == 0:
        filter_t = random.choice(tables_filter)
        columns_filter = list(dict_tables_columns[filter_t].keys())
        filter_c = random.choice(columns_filter)
        values = dict_tables_columns[filter_t][filter_c]
        filter_value = random.choice(values)
        t_c = tables_filter.index(filter_t)+1
        query_where += " t{}.{} = $${}$$;".format(t_c, filter_c, filter_value)
    else:
        n_table_filter = fast_randint(len(tables_filter))+1
        for i in range(n_table_filter):
            current_table = tables_filter[i]
            columns_filter = list(dict_tables_columns[current_table].keys())
            n_columns_filter = fast_randint(3)+1
            for n in range(n_columns_filter):
                filter_c = random.choice(columns_filter)
                columns_filter.remove(filter_c)
                values = dict_tables_columns[current_table][filter_c]
                filter_value = random.choice(values)
                t_c = tables_filter.index(current_table)+1
                query_where += " t{}.{} = $${}$$ and".format(t_c, filter_c, filter_value)
        query_where = query_where[:-3] + ';'
    return query_where

def random_violate_tuple(tuple_attributes, tuple_table, dict_tables_columns, cursor):
    tuple = []
    index = []
    for t in range(len(tuple_attributes)):
        tuple_att = tuple_attributes[t]
        rows = dict_tables_columns[tuple_table][tuple_att]
        if t == 0:
            ind = fast_randint(len(rows))
            index.append(ind)
            index = str(index).strip('[]')
            index = int(index)
        tuple.append(rows[index])
    return tuple

def random_query(database, primary_keys_multi):
    cursor, tableNames, dict_attributesNtypes = getAttributesNtypes(database)
    # extract the attributes from dict_attributesNtypes,
    # so that we don't need to use fetchall again, since it takes lots of time
    dict_tables_columns, dict_attributes, dict_tables, dict_primary_keys = dictionariesFormation(tableNames, dict_attributesNtypes, primary_keys_multi, cursor)
    # now start query formation
    dict_tables_join = joinPreparation(tableNames, dict_attributes)
    while(True):
        # query from part
        tables_filter, query_from, j_attributes = joinFormation(tableNames, dict_tables_join)
        # query select part
        query_select, tuple_attributes, tuple_table = selectFormation(tables_filter, dict_attributes, j_attributes, dict_primary_keys)
        # query filter part
        query_where = filterFormation(tables_filter, dict_tables_columns)
        # concatenate them together
        query = query_select + query_from + query_where
        try:
            cursor.execute(query)
            if (len(cursor.fetchall()) == 0) or (cursor.fetchall() == None):
                continue
            else:
                break
        except:
            None
    print(tuple_attributes)
    # generate tuple according to the query_select
    tuple = random_violate_tuple(tuple_attributes, tuple_table, dict_tables_columns, cursor)
    return dict_tables, dict_attributesNtypes, tables_filter, dict_attributes, query, tuple

if __name__ == "__main__":
        for n in range(1):
            dict_tables, dict_attributesNtypes, tables_filter, dict_attributes, query, tuple = random_query("lobbyists_db", [('client_id',),('compensation_id',),('contribution_id',),('employer_id',),('gift_id',),('lobbying_activity_id',),('lobbyist_id',)])
            print(query)
            print(tuple)
