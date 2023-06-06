import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT



con = psycopg2.connect(database="snapshot",
                       user="postgres",
                       password="12345678",
                       host="localhost",
                       port="5433")
con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cursor = con.cursor()

init_file = open('./init.sql', 'r')
queries = init_file.readlines()
init_file.close()
for line in queries:
    cursor.execute(line)
