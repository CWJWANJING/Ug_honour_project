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
    # random select from 'inner join', 'left join', 'right join', 'full join', 'comma join'
    opt_pre = ['inner join', 'left join', 'right join', 'full join', 'comma join']
    n_opt_pre = fast_randint(5)
    # +1 because we need at least one table to be selected
    n_tables = fast_randint(len(tableNames))+1
    tables_filter = []
    j_attributes = []
    query_from = " from"
    if n_opt_pre == 4:
        # select random number of tables in the database
        random_index_tables = np.unique(np.random.randint(1, len(tableNames), size=n_tables))
        random_tables = [tableNames[i] for i in random_index_tables]
        for i in range(len(random_tables)):
            query_from += f" {random_tables[i]} t{i+1},"
        tables_filter = random_tables
        query_from = query_from[:-1]
    else:
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

def selectFormation(tables_filter, dict_attributes, j_attributes, dict_primary_keys):
    # select randomly from ''distinct', 'random columns'
    n_opt_2 = fast_randint(2)
    columns_select = []
    dict_vio_select = {}
    for t in tables_filter:
        n_columns = fast_randint(len(dict_attributes[t])-1)+1
        columns_select.append(dict_attributes[t][n_columns])
        dict_vio_select[t] = dict_attributes[t][n_columns]
    n_prim = fast_randint(len(tables_filter))
    tuple_table = tables_filter[n_prim]
    columns_prims = dict_primary_keys[tuple_table]
    print(f"columns_prims {columns_prims} \n")
    print(f"dict_vio_select {dict_vio_select}")
    list2 = []
    list2.append(dict_vio_select[tuple_table])
    print(f"list2: {list2} \n")
    tuple_attributes = list(set(columns_prims) | set(list2))
    print(f"tuple: {tuple_attributes} \n")
    columns_select = set(columns_select)
    if j_attributes != []:
        common = list(set(j_attributes) & set(columns_select))
        if common != []:
            comm = list(set(common)&set(columns_prims))
            if comm != []:
                columns_prims = [x for x in columns_prims if x not in comm]
                print(columns_prims)
                print("1")
                common = ["t1." + s for s in common]
                common = ", ".join(common)
                columns_select = [x for x in columns_select if x not in common]
                columns_select = ", ".join(columns_select)
                if columns_prims != []:
                    print("2")
                    columns_prims = [x for x in columns_prims]
                    columns_prims = ", ".join(columns_prims)
                    if n_opt_2 == 0:
                        if columns_select != {}:
                            query_select = f"select {common}, {columns_prims}, {columns_select}"
                        else:
                            query_select = f"select {columns_prims}, {columns_select}"
                    else:
                        if columns_select != {}:
                            query_select = f"select distinct {common}, {columns_prims}, {columns_select}"
                        else:
                            query_select = f"select distinct {columns_prims}, {columns_select}"
                else:
                    print("3")
                    if n_opt_2 == 0:
                        if columns_select != {}:
                            query_select = f"select {common}, {columns_select}"
                        else:
                            query_select = f"select {columns_select}"
                    else:
                        if columns_select != {}:
                            query_select = f"select distinct {common}, {columns_select}"
                        else:
                            query_select = f"select distinct {columns_select}"
            else:
                common = ["t1." + s for s in common]
                common = ", ".join(common)
                columns_select = [x for x in columns_select if x not in common]
                columns_prims = [x for x in columns_prims]
                columns_prims = ", ".join(columns_prims)
                print(columns_prims)
                print("4")
                if n_opt_2 == 0:
                    if columns_select != []:
                        print("8")
                        columns_select = ", ".join(columns_select)
                        print(common+columns_prims+columns_select)
                        query_select = f"select {common}, {columns_prims}, {columns_select}"
                    else:
                        print("7")
                        query_select = f"select {columns_prims} "
                else:
                    if columns_select != []:
                        print("5")
                        columns_select = ", ".join(columns_select)
                        query_select = f"select distinct {common}, {columns_prims}, {columns_select}"
                    else:
                        print("6")
                        query_select = f"select distinct {columns_prims} "
        else:
            columns_select = ", ".join(columns_select)
            if n_opt_2 == 0:
                query_select = f"select {columns_select}"
            else:
                query_select = f"select distinct {columns_select}"
    else:
        columns_select = ", ".join(columns_select)
        if n_opt_2 == 0:
            query_select = f"select {columns_select}"
        else:
            query_select = f"select distinct {columns_select}"
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
        query_where += " t{}.{} = '{}';".format(t_c, filter_c, filter_value)
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
                query_where += " t{}.{} = '{}' and".format(t_c, filter_c, filter_value)
        query_where = query_where[:-3] + ';'
    return query_where

def random_violate_tuple(tuple_attributes, tuple_table, dict_tables_columns):
    tuple = []
    print(tuple_table)
    for t in range(len(tuple_attributes)):
        tuple_att = tuple_attributes[t]
        rows = dict_tables_columns[tuple_table][tuple_att]
        index = fast_randint(len(rows))
        tuple.append(rows[index])
    print(tuple)
    return tuple

def random_query(database, primary_keys_multi):
    cursor, tableNames, dict_attributesNtypes = getAttributesNtypes(database)
    # extract the attributes from dict_attributesNtypes,
    # so that we don't need to use fetchall again, since it takes lots of time
    dict_tables_columns, dict_attributes, dict_tables, dict_primary_keys = dictionariesFormation(tableNames, dict_attributesNtypes, primary_keys_multi, cursor)
    # now start query formation
    dict_tables_join = joinPreparation(tableNames, dict_attributes)
    # query from part
    tables_filter, query_from, j_attributes = joinFormation(tableNames, dict_tables_join)
    # query select part
    query_select, tuple_attributes, tuple_table = selectFormation(tables_filter, dict_attributes, j_attributes, dict_primary_keys)
    # query filter part
    query_where = filterFormation(tables_filter, dict_tables_columns)
    # concatenate them together
    query = query_select + query_from + query_where

    tuple = random_violate_tuple(tuple_attributes, tuple_table, dict_tables_columns)
    print(tuple)
    return dict_tables, dict_attributesNtypes, tables_filter, dict_attributes, query

if __name__ == "__main__":
        dict_tables, dict_attributesNtypes, tables_filter, dict_attributes, query = random_query("lobbyists_db", [('client_id',),('compensation_id',),('contribution_id',),('employer_id',),('gift_id',),('lobbying_activity_id',),('lobbyist_id',)])
        print(query)
