import sys
import time
import os

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

try:
    with mariadb.connect(**SQL_CONN_CONFIG) as conn:
        
        with conn.cursor() as cur:

            # 1. insert update_runs
            cur.execute(
                "INSERT INTO update_runs (update_time) VALUES (NOW())"
            )
            update_id = cur.lastrowid

            # 2. prepare delays data
            delays_data = [
                (1, update_id, 120),
                (2, update_id, 0),
                (3, update_id, 45),
            ]

            # 3. bulk insert delays
            cur.executemany(
                """
                INSERT INTO delays (station_id, update_id, avg_delay)
                VALUES (?, ?, ?)
                """,
                delays_data,
            )

        # commit in success only
        conn.commit()

except mariadb.Error as e:
    conn.rollback()  # Just in case
    print(f'MariaDB error: {e}')
    raise
    



raise SystemExit(1)

##################################################################
##################################################################
##################################################################

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
    
    DEBUG = 1
    
    SHOW_PROGRESS_BAR = 0
    
    SHOW_SIMPLE_PROGRESS = 0

    #########################
    
    if( DEBUG ):
        max_count = 20
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
        
        time.sleep(0.5)
        
        ##################################################
        ##################################################
        ##################################################
        
        ###  Parse answer  ###
        
        delays_sec = []
        
        for section in ("departures", "arrivals"):
            items = liveboard_data.get(section, {}).get("departure") \
                    or liveboard_data.get(section, {}).get("arrival")
            if not items:
                continue
        
            if isinstance(items, dict):
                items = [items]
        
            for item in items:
                # delay is a string with seconds ("0", "60", "-60", ...)
                d = int(item.get("delay", "0"))
                if d < 0:
                    d = 0  # early is not a "delay" for station-problem scoring
                delays_sec.append(d)
        
        # Average delay for the station (including trains with 0 delay)
        avg_delay_sec = sum(delays_sec) / len(delays_sec) if delays_sec else 0.0
        avg_delay_min = avg_delay_sec / 60
        
        # Delays >= 2 minutes (filter list)
        delays_2min_plus_sec = [d for d in delays_sec if d >= 120]
        
        print(f"Average delay: {avg_delay_min:.2f} min")
        print(f"Count delays >= 2 min: {len(delays_2min_plus_sec)}")

        # delays_sec is already built (list of non-negative delays in seconds)
        
        total = len(delays_sec)
        delayed_2min = [d for d in delays_sec if d >= 120]
        
        share_2min = (len(delayed_2min) / total) if total else 0.0
        
        avg_delay_delayed_sec = (sum(delayed_2min) / len(delayed_2min)) if delayed_2min else 0.0
        avg_delay_delayed_min = avg_delay_delayed_sec / 60
        
        print(f"Share delays >= 2 min: {share_2min:.3f}")
        print(f"Avg delay among >= 2 min trains: {avg_delay_delayed_min:.2f} min")
        
        print('\n\n\n')
        

finally:
    
    lock.release()    

if(SHOW_SIMPLE_PROGRESS):
    print('\n')

##################################################################
##################################################################
##################################################################


##################################################################
##################################################################
##################################################################

end_time = time.strftime("%d.%m.%Y - %H:%M:%S", time.localtime())

print(f'\nFinished at: {end_time}')

print('\nJob finished!\n')

