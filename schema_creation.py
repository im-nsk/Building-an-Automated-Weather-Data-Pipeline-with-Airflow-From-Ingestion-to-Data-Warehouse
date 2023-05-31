# import required module
import psycopg2 as pg
# import configparser as config

# Establish connection to the AWS REDSHIFT(Data warehouse)
def establish_connection():
    # get the credential through config file
    # config = config.ConfigParser()
    # config.read_file(open('config_file.ini'))

    # host_name = config.get('AWS', 'HOST_NAME')
    # port_no = config.get('AWS', 'PORT_NO')
    # db_name = config.get('AWS', 'DB_NAME')
    # user_name = config.get('AWS', 'USER_NAME')
    # password = config.get('AWS', 'PASSWORD')

    host_name = 'datapipeline-cluster.270149585494.us-east-1.redshift-serverless.amazonaws.com'
    port_no = 5439
    db_name = 'dev'
    user_name = 'admin'
    password = 'Nn12345678'

    try:
        conn = pg.connect(host = host_name, dbname = db_name, port = port_no, user = user_name, password = password)
        print("Connection Established")
    except pg.Error as e:
        print(e)
    
    # create cursor to execute sql query
    cur = conn.cursor()
    return conn, cur

 # Create table schema in REDHSIFT data-base
def create_schema():
    conn, cur = establish_connection()
    table_creation_query = "CREATE TABLE IF NOT EXISTS weather_table(country varchar(15),city_name varchar(30), temperature float, sunrise DATE, sunset DATE, timezone DATE)"
    try:
        cur.execute(table_creation_query)
        print('Table created')
    except pg.Error as e:
        print(e)
    conn.commit()

    close_connection(conn, cur)

 # Once all done, can be checked data from the table
def display_table_data(cur):
    cur.execute("SELECT * FROM weather_table")
    data = cur.fetchall()
    for row in data:
        print(row)

def close_connection(conn, cur):
    cur.close()
    conn.close()



create_schema()
# display_table_data()



        