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

if sys.platform.startswith('linux'):  # for my VM
    SQL_CONN_CONFIG['host'] = 'localhost'


##################################################################
##################################################################
##################################################################

def combine_databases():

    print('Combining databases begins.')
    
    ###  Define data_generation  ###
    
    data_generation = 0
    
    try:
        with mariadb.connect(**SQL_CONN_CONFIG) as conn:
    
            cursor = conn.cursor()
            
            cursor.execute('SELECT MAX(data_generation) FROM all_trains')
            value = cursor.fetchone()[0]
            
            if value is None:
                data_generation = 0
            else:
                data_generation = int(value)
            
    except mariadb.Error as e:
        print(f'MariaDB error: {e}')
    
    data_generation += 1  # The next generation
    
    ##################################################################
    ##################################################################
    ##################################################################
    
    ###  Get IDS from two DBs  ###
    
    with mariadb.connect(**SQL_CONN_CONFIG) as conn:
        
        ###  Get NEW trains ids  ###
        
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT new_trains.train_id
            FROM new_trains
            WHERE NOT EXISTS (
                SELECT 1
                FROM all_trains
                WHERE all_trains.train_id = new_trains.train_id
            )
        ''')
        
        NEW_TRAINS_IDS = [row[0] for row in cursor.fetchall()]
        
        ###  Get INNER JOIN trains ids  ###
     
        cursor.execute('''
            SELECT new_trains.train_id
            FROM new_trains
            INNER JOIN all_trains
                ON all_trains.train_id = new_trains.train_id;
        ''')
        
        OLD_ACTIVE_TRAINS_IDS = [row[0] for row in cursor.fetchall()]
        
        ###  Get OBSOLATED trains ids  ###
     
        cursor.execute('''
            SELECT all_trains.train_id
            FROM all_trains
            WHERE NOT EXISTS (
                SELECT 1
                FROM new_trains
                WHERE new_trains.train_id = all_trains.train_id
            );
        ''')
        
        OUTDATE_TRAINS_IDS = [row[0] for row in cursor.fetchall()]
    
        ##################################################################
        ##################################################################
        ##################################################################
        
        ###  UPDATE ALL_TRAINS TABLE  ###
        
        ###  Set active:
        
        if OLD_ACTIVE_TRAINS_IDS:
        
            placeholders = ','.join(['%s'] * len(OLD_ACTIVE_TRAINS_IDS))
            
            sql = f'''
                UPDATE all_trains
                SET train_status = 1
                WHERE train_id IN ({placeholders})
            '''
            
            cursor.execute(sql, OLD_ACTIVE_TRAINS_IDS)
            
            conn.commit()
    
        ###  Set outdate:
        
        if OUTDATE_TRAINS_IDS:
            
            placeholders = ','.join(['%s'] * len(OUTDATE_TRAINS_IDS))
            
            sql = f'''
                UPDATE all_trains
                SET train_status = 0
                WHERE train_id IN ({placeholders})
            '''
            
            cursor.execute(sql, OUTDATE_TRAINS_IDS)
            
            conn.commit()
        
        ### Add NEW trains:
        
        if NEW_TRAINS_IDS:
            
            print(f'New Data Generation: {data_generation}')
            
            print(f'Add {len(NEW_TRAINS_IDS)} new trains.')
            
            rows = [(train_id, data_generation, 1) for train_id in NEW_TRAINS_IDS]
            
            cursor.executemany(
                """
                INSERT INTO all_trains (train_id, data_generation, train_status)
                VALUES (%s, %s, %s)
                """,
                rows
            )
            
            conn.commit()
        
        else:
        
            print('No new trains found.')
        
        
    ##################################################################
    ##################################################################
    ##################################################################
    
    print('Combining databases finished.')



