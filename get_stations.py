import sys

import fasteners
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

BLOCK_TABLE = 1  # Please don't run this script !

if BLOCK_TABLE:
    
    print("\nPlease don't run this script!\n")
    raise SystemExit(1)
    

resource_path = 'stations'

url = f'{config.BASE_URL}/{resource_path}/'

lock_path = services.get_lock_path('irail')

lock = fasteners.InterProcessLock(lock_path)

if not lock.acquire(blocking=False):
    print('Process blocked: too many requests, please wait...')
    raise SystemExit(1)

try:

    data = services.iRailRequest(url)

finally:
    lock.release()    

##################################################################

# According to iRail API, "station" may be a list or a single object
stations_data = data.get('station')
if stations_data is None:
    print('No stations field in response')
    raise SystemExit(1)

# Normalize to list for consistent processing
if isinstance(stations_data, dict):
    stations = [stations_data]
else:
    stations = stations_data

##################################################################
##################################################################
##################################################################

print('Total stations count:', len(stations))


stations_data = [
            (
             s.get('id'),
             s.get('name'), 
             s.get('standardname'), 
             s.get('locationX'), 
             s.get('locationY') 
            ) for s in stations]

try:
    # Unpacking a dictionary into keyword arguments!
    with mariadb.connect(**SQL_CONN_CONFIG) as conn:

        conn.autocommit = False
        
        cursor = conn.cursor()
        
        cursor.execute('DELETE FROM stations')
        
        cursor.execute('ALTER TABLE stations AUTO_INCREMENT = 1')
        
        cursor.executemany(
            'INSERT INTO stations (station_id, name, standardname, lon, lat) VALUES (%s, %s, %s, %s, %s)',
            stations_data  # list of tuples
        )
        
        conn.commit()
                
except mariadb.Error as e:
    
    print(f"DB error: {e}")
    
##################################################################

print('\nJob finished!')

