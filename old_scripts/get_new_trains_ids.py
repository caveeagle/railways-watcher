import sys
import time
import os

import fasteners
import mariadb

import config
import config_secrets
import services
import combine_trains

##################################################################

# Change to your own parameters #

SQL_CONN_CONFIG = {
    'user': config_secrets.WRITE_USER,
    'password': config_secrets.WRITE_USER_PWD,
    'host': config_secrets.SERVER_IP,
    'database': config_secrets.DB_NAME,
    'port': 3306 # by default
}

if sys.platform.startswith('linux'):  # for my VM
    SQL_CONN_CONFIG['host'] = 'localhost'


##################################################################
##################################################################
##################################################################

if hasattr(time, "tzset"):
    os.environ["TZ"] = "Europe/Brussels"
    time.tzset()
    
now = time.strftime("%d.%m.%Y - %H:%M:%S", time.localtime())

print(f'\nStart at: {now}')

### Get stations IDs from DB ###

stations_ids = []
 
try:
    # Unpacking a dictionary into keyword arguments!
    with mariadb.connect(**SQL_CONN_CONFIG) as conn:

        cur = conn.cursor(dictionary=True)

        cur.execute(f'SELECT * FROM stations')

        rows = cur.fetchall()
        
        stations_ids = [row['station_id'] for row in rows]
        
except mariadb.Error as e:
    print(f'MariaDB error: {e}')

##################################################################
##################################################################
##################################################################

### Function to parse liveboards data

def get_trains_ids(liveboard_data):

    trains_ids = set()
    
    for section in ('departures', 'arrivals'):
        items = liveboard_data.get(section, {}).get('departure') \
                or liveboard_data.get(section, {}).get('arrival')
    
        if not items:
            continue
    
        if isinstance(items, dict):
            items = [items]
    
        for item in items:
            vid = (
                item.get('vehicle')
                or item.get('vehicleinfo', {}).get('name')
            )
            if vid:
                trains_ids.add(vid)

    return trains_ids

##################################################################
##################################################################
##################################################################


NEW_TRAINS_IDS = set()

resource_path = 'liveboard'

url = f'{config.BASE_URL}/{resource_path}/'

lock_path = services.get_lock_path('irail')

lock = fasteners.InterProcessLock(lock_path)

if not lock.acquire(blocking=False):
    print('Process blocked')
    raise SystemExit(1)



try:

    s_count = 1
    
    #########################
    
    DEBUG = 0
    
    SHOW_PROGRESS_BAR = 1
    
    SHOW_SIMPLE_PROGRESS = 0

    #########################
    
    if( DEBUG ):
        max_count = 40
    else:
        max_count = len(stations_ids)
    
    
    if(SHOW_PROGRESS_BAR):

        from tqdm import tqdm
        
        pbar = tqdm(
            total=max_count,
            desc='Processing',
            bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} ({percentage:3.0f}%)'
        )
        
        import atexit
        
        atexit.register(pbar.close)  # Wow!
    
    if(SHOW_SIMPLE_PROGRESS):
        print('Progress(%): 0', end='', flush=True)
        
        
    for station_id in stations_ids:
    
        s_count += 1
        
        if(SHOW_PROGRESS_BAR):
            pbar.update(1)
            
        if(SHOW_SIMPLE_PROGRESS):
            p_count = s_count*100//max_count
            if( (p_count%5 == 0)and(p_count%10 != 0) ):            
                print('.', end='', flush=True)
            if( p_count%10 == 0 ):
                print(p_count//10, end='', flush=True)            
            
        if( DEBUG ):
            if( s_count>max_count ):
                break

        params = {}
        
        params['id'] = station_id
        
        liveboard_data = services.iRailRequest(url,params)
        
        NEW_TRAINS_IDS.update( get_trains_ids(liveboard_data) )
        
        time.sleep(0.5)

finally:
    
    lock.release()    

if(SHOW_SIMPLE_PROGRESS):
    print('\n')

print(f'\nTRAINS FOUND: {len(NEW_TRAINS_IDS)}\n')

##################################################################
##################################################################
##################################################################

data = [(train_id,) for train_id in NEW_TRAINS_IDS]

try:

    with mariadb.connect(**SQL_CONN_CONFIG) as conn:

        conn.autocommit = False
        
        cursor = conn.cursor()
        
        cursor.execute('DELETE FROM new_trains')
        
        cursor.executemany(
            'INSERT INTO new_trains (train_id) VALUES (%s)',
            data
        )
        
        conn.commit()
                
except mariadb.Error as e:
    
    print(f'DB error: {e}')

##################################################################
##################################################################
##################################################################

combine_trains.combine_databases()

##################################################################
##################################################################
##################################################################

end_time = time.strftime("%d.%m.%Y - %H:%M:%S", time.localtime())

print(f'\nFinished at: {end_time}')

print('\nJob finished!\n')

