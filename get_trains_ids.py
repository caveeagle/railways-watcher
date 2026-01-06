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

ids = []
 
try:
    # Unpacking a dictionary into keyword arguments!
    with mariadb.connect(**SQL_CONN_CONFIG) as conn:

        cur = conn.cursor(dictionary=True)

        cur.execute(f'SELECT * FROM stations')

        rows = cur.fetchall()
        
        ids = [row['station_id'] for row in rows]
        
except mariadb.Error as e:
    print(f"MariaDB error: {e}")

##################################################################

resource_path = 'liveboard'

url = f'{config.BASE_URL}/{resource_path}/'

lock_path = services.get_lock_path('irail')

lock = fasteners.InterProcessLock(lock_path)

if not lock.acquire(blocking=False):
    print('Process blocked: too many requests, please wait...')
    raise SystemExit(1)

try:
    
    params = {}
    
    params['id'] = 'BE.NMBS.008811940'
    
    liveboard_data = services.iRailRequest(url,params)

finally:
    lock.release()    

##################################################################


vehicle_ids = set()

for section in ("departures", "arrivals"):
    items = liveboard_data.get(section, {}).get("departure") \
            or liveboard_data.get(section, {}).get("arrival")

    if not items:
        continue

    if isinstance(items, dict):
        items = [items]

    for item in items:
        vid = (
            item.get("vehicle")
            or item.get("vehicleinfo", {}).get("name")
        )
        if vid:
            vehicle_ids.add(vid)


print(vehicle_ids)
    
##################################################################

print('\nJob finished!')

