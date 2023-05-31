# import required module
import psycopg2 as pg
import configparser as config

# get the credential through config file
config = config.ConfigParser()
config.read_file(open('config_file.ini'))

HOST_NAME = config.get('AWS', 'HOST_NAME')
PORT_NO = config.get('AWS', 'PORT_NO')
DB_NAME = config.get('AWS', 'DB_NAME')
USER_NAME = config.get('AWS', 'USER_NAME')
PASSWORD = config.get('AWS', 'PASSWORD')

def establish_connection():

    # Establish connection to the AWS REDSHIFT(Data warehouse)
    try:
        conn = pg.connect(host = HOST_NAME, dbname = DB_NAME, port = USER_NAME, user = USER_NAME, password = PASSWORD)
        print("Connection Established")
    except pg.Error as e:
        print(e)
    
    # create cursor to execute sql query
    cur = conn.cursor()
    return conn, cur

def create_schema():
    # before creating schema, connection establishment required
    conn, cur = establish_connection()
    
     # Create table schema in REDSHIFT acc. to ingest data.
    table_creation_query = "CREATE TABLE IF NOT EXISTS weather_table(country varchar(15),city_name varchar(30), temperature float, sunrise TIME, sunset TIME, timezone TIME)"
    try:
        cur.execute(table_creation_query)
        print('Table created')
    except pg.Error as e:
        print(e)
    conn.commit()
    
    #close connection
    close_connection(conn, cur)


def close_connection(conn, cur):
    cur.close()
    conn.close()


#run the function Manually if needed or else not needed
create_schema()
