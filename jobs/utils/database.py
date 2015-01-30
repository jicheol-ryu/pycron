import pymysql


class DataBase:
    class Configs:
        def __init__(self):
            return
        TEST_DB = {'charset': 'utf8',
                'host': '127.0.0.1', 'port': 3306, 'db': 'test',
                'user': 'user', 'passwd': 'password'}

    def __init__(self, db_config):
        self.__conn = pymysql.connect(**db_config)

    def execute_query(self, query, *args):
        cursor = self.__conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute(query, args)
        result = cursor.fetchall()
        return result

    def execute_update(self, query, *args):
        cursor = self.__conn.cursor()
        cursor.execute(query, args)
        self.__conn.commit()

    def insert_datalist_to_table(self, table_name, columns, datalist, duplicate_update_column=None):
        if 0 >= len(datalist):
            return
        sql = u'INSERT INTO `%s` ' % table_name

        sql_columns = u'('
        values = u'('
        for column in columns:
            sql_columns += u'`%s`,' % column
            values += u'%s,'
        sql_columns = sql_columns[:-1] + u')'
        values = values[:-1] + u'),'
        sql += sql_columns + u' VALUES '

        arg_list = list()
        for data in datalist:
            sql += values
            for column in columns:
                arg_list.append(data[column])
        sql = sql[:-1]
        if duplicate_update_column and 0 < len(duplicate_update_column):
            sql += u' ON DUPLICATE KEY UPDATE '
            for column in duplicate_update_column:
                sql += '`%s`=VALUES(`%s`),' % (column, column)
            sql = sql[:-1]

        self.execute_update(sql, *tuple(arg_list))

    def close(self):
        self.__conn.close()
