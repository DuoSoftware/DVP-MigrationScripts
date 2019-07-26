import difflib
import os
import sys
import PostgresHandler as pg
import configs.ConfigHandler as conf
out = conf.get_conf('config.ini','output')

def get_db_tables(pg1):
    tables = set()
    res = pg1.execute_query('SELECT schemaname, tablename FROM pg_catalog.pg_tables')
    for table in res:
        if table['schemaname'] == 'public':
            tables.add(table['tablename'])
    return tables


def compare_number_of_items(db_obj, db1_items, db2_items, items_name):

    if db1_items != db2_items:
        additional_db1 = db1_items - db2_items
        additional_db2 = db2_items - db1_items

        if additional_db1:
            print('{}: additional in "{}"\n'.format(items_name, 'db1'))
            for t in additional_db1:
                print('\t{}\n'.format(t))
            print('\n')

        if additional_db2:
            print('{}: additional in "{}"\n'.format(items_name, 'db2'))
            for t in additional_db2:
                print('\t{}\n'.format(t))
            print('\n')

def get_table_definition(db, table_name):
    res = db.execute_query("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{0}'".format(table_name))
    definition = sorted(res, key=lambda k: k['column_name'])
    # columns = sorted([d['column_name'] for d in res if 'column_name' in d])
    return definition

def compare_each_table(db_objs, db1_tables, db2_tables, items_name):
    not_matching_tables = []

    for t in sorted(db1_tables & db2_tables):
        t1 = get_table_definition(db_objs['db1'], t)
        t2 = get_table_definition(db_objs['db2'], t)
        if t1 != t2:
            not_matching_tables.append(t)

            t1list = []
            for _ in t1:
                d = [str(v) for k, v in _.items()]
                t1list.append(' '.join(d))
            #t1list = '\n'.join(t1list)

            t2list = []
            for _ in t2:
                d = [str(v) for k, v in _.items()]
                t2list.append(' '.join(d))
            #t2list = '\n'.join(t2list)

            diff = difflib.unified_diff(
                t1list,
                t2list,
                '{}.{}.{}'.format(items_name, 'db1', t),
                '{}.{}.{}'.format(items_name, 'db2', t),
                n=sys.maxsize
            )

            for _ in diff:
                print(_)

            if out['location']:
                if not os.path.exists(out['location']):
                    os.mkdir(out['location'])
                filepath = os.path.join(
                    out['location'], '{}.diff'.format(t)
                )
                with open(filepath, 'w') as f:
                    for diff_line in diff:
                        f.write(diff_line)

    if not_matching_tables:
        print('{}: not matching\n'.format(items_name))
        for t in not_matching_tables:
            print('\t{}\n'.format(t))
        print('\n')

if __name__ == "__main__":
    pg1 = pg.PostgresHandler('db1')
    pg2 = pg.PostgresHandler('db2')
    pg1.initiate()
    pg2.initiate()
    db_objs = {'db1': pg1, 'db2': pg2}

    db1_tables = get_db_tables(pg1)
    db2_tables = get_db_tables(pg2)

    compare_number_of_items(pg1, db1_tables, db2_tables, 'TABLES')
    compare_each_table(db_objs, db1_tables, db2_tables, 'TABLES')

# https://pypi.org/project/postgres-db-diff/