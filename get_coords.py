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

### Get trains IDs from DB ###

TRAINS_IDS = []
 
try:
    with mariadb.connect(**SQL_CONN_CONFIG) as conn:

        cursor = conn.cursor()

        #cursor.execute(f'SELECT train_id FROM all_trains WHERE train_status = 1 ORDER BY updated_at DESC')

        cursor.execute(f'SELECT train_id FROM all_trains WHERE data_generation = 7 ORDER BY updated_at DESC')

        TRAINS_IDS = [row[0] for row in cursor.fetchall()]
        
except mariadb.Error as e:
    print(f'MariaDB error: {e}')

##################################################################
##################################################################
##################################################################

resource_path = 'vehicle'

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
        max_count = 50
    else:
        max_count = len(TRAINS_IDS)
    
    
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
        
        
    for train_id in TRAINS_IDS:
    
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
        
        params['id'] = train_id
        
        train_data = services.iRailRequest(url,params)
        
        time.sleep(0.5)
        
        if train_data is None:
            continue
        
        vehicleinfo = train_data.get("vehicleinfo") or {}
        
        lon = float(vehicleinfo.get("locationX") or 0)
        lat = float(vehicleinfo.get("locationY") or 0)
        
        if lon and lat:
            print(f'\nCOORDINATES: {lon} - {lat}\n') 

finally:
    
    lock.release()    

if(SHOW_SIMPLE_PROGRESS):
    print('\n')

##################################################################
##################################################################
##################################################################

print('\nJob finished!\n')

