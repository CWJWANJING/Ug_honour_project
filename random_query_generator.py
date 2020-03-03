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
    # put values for each attribute into corresponding dictionary
    # use nested dictionary in this case
    dict_tables_columns = {}
    for i in range(len(columns)):
        current_columns = columns[i]
        dict_columns = {}
        for m in range(len(columns[i])):
            dict_columns[current_columns[m]] = [row_values[m] for row_values in dict_tables[tableNames[i]]]
        dict_tables_columns[tableNames[i]] = dict_columns
    return dict_tables_columns, dict_attributes

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
    if n_opt_pre == 4:
        # select random number of tables in the database
        random_index_tables = np.unique(np.random.randint(1, len(tableNames), size=n_tables))
        random_tables = [tableNames[i] for i in random_index_tables]
        query_from = f" from {', '.join(random_tables)} "
        tables_filter = random_tables
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
    return tables_filter, query_from

def selectFormation(tables_filter, dict_attributes):
    # select randomly from 'all', 'distinct', 'random columns'
    # opt_1 = ['*', 'random_columns']
    # 1 represents for *, and else random_columns
    n_opt_1 = fast_randint(2)
    columns_select = ""
    if n_opt_1 == 1:
        n_opt_2 = fast_randint(2)
        columns_select = []
        for t in tables_filter:
            # doesn't consider cases when attribute names in tables collapse
            n_columns = fast_randint(len(dict_attributes[t]))
            columns_select.append(dict_attributes[t][n_columns])
        columns_select = ', '.join(columns_select)
        if n_opt_2 == 0:
            query_select = f"select {columns_select}"
        else:
            query_select = f"select distinct {columns_select}"
    else:
        query_select = f"select *"
    return query_select

def filterFormation(tables_filter, dict_tables_columns):
    # randomly filter the values
    # randomly decide to filter how many set of values
    filter_times = fast_randint(2)
    query_where = " where "
    if filter_times == 0:
        filter_t = random.choice(tables_filter)
        columns_filter = list(dict_tables_columns[filter_t].keys())
        filter_c = random.choice(columns_filter)
        values = dict_tables_columns[filter_t][filter_c]
        filter_value = random.choice(values)
        query_where += ' where {} = "{}";'.format(filter_c, filter_value)
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
                query_where += ' {} = "{}" and'.format(filter_c, filter_value)
        query_where = query_where[:-3] + ';'
    return query_where

def random_query(database, primary_keys_multi):
    cursor, tableNames, dict_attributesNtypes = getAttributesNtypes(database)
    # extract the attributes from dict_attributesNtypes,
    # so that we don't need to use fetchall again, since it takes lots of time
    dict_tables_columns, dict_attributes = dictionariesFormation(tableNames, dict_attributesNtypes, primary_keys_multi, cursor)
    # now start query formation
    dict_tables_join = joinPreparation(tableNames, dict_attributes)
    # query from part
    tables_filter, query_from = joinFormation(tableNames, dict_tables_join)
    # query select part
    query_select = selectFormation(tables_filter, dict_attributes)
    # query filter part
    query_where = filterFormation(tables_filter, dict_tables_columns)
    # concatenate them together
    query = query_select + query_from + query_where
    return query

if __name__ == "__main__":
    for n in range(20):
        print(random_query("lobbyists_db", [('client_id',),('compensation_id',),('contribution_id',),('employer_id',),('gift_id',),('lobbying_activity_id',),('lobbyist_id',)]))
