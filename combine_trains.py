import sys
import time

import mariadb

import config
import config_secrets
import services

##################################################################

# Change to your own parameters #

SQL_CONN_CONFIG = {
    'user': config_secrets.WRITE_USER,
    'password': config_secrets.WRITE_USER_PWD,
    'host': config_secrets.SERVER_IP,
    'database': config_secrets.DB_NAME,
    'port': 3306 # by default
}

if sys.platform.startswith("linux"):  # for my VM
    SQL_CONN_CONFIG['host'] = 'localhost'


##################################################################
##################################################################
##################################################################

### Define data_generation  ###

data_generation = 0

try:
    with mariadb.connect(**SQL_CONN_CONFIG) as conn:

        cursor = conn.cursor()
        
        cursor.execute("SELECT MAX(data_generation) FROM all_trains")
        value = cursor.fetchone()[0]
        
        if value is None:
            data_generation = 0
        else:
            data_generation = int(value)
        
except mariadb.Error as e:
    print(f"MariaDB error: {e}")

data_generation += 1  # The next generation

print(f'data_generation={data_generation}')

##################################################################
##################################################################
##################################################################

print('\nJob finished!')

