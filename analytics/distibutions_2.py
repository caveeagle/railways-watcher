import sys

import mariadb
import pandas as pd
import matplotlib.pyplot as plt

import config_secrets

#########################################################

VALUES = []

SQL_CONN_CONFIG = {
    'user': config_secrets.WRITE_USER,
    'password': config_secrets.WRITE_USER_PWD,
    'host': config_secrets.SERVER_IP,
    'database': config_secrets.DB_NAME,
    'port': 3306 # by default
}

if sys.platform.startswith('linux'):  # for my VM
    SQL_CONN_CONFIG['host'] = 'localhost'

#########################################################

SQL_QUERY = '''
                SELECT
                    update_runs.update_time,
                    AVG(delays.avg_delay) AS avg
                FROM update_runs
                JOIN delays
                  ON delays.update_id = update_runs.update_id
                WHERE delays.avg_delay <> 0
                GROUP BY
                    update_runs.update_id,
                    update_runs.update_time
                ORDER BY
                    update_runs.update_time;
               '''

#########################################################

try:
    with mariadb.connect(**SQL_CONN_CONFIG) as conn:

        cur = conn.cursor(dictionary=True)
        
        cur.execute(SQL_QUERY)

        rows = cur.fetchall()
        
        VALUES = [(row['update_time'], row['avg']) for row in rows]
        
        
except mariadb.Error as e:
    print(f'MariaDB error: {e}')
        
print(f'Number of values: {len(VALUES)}')

    
#########################################################
#########################################################

# DataFrame
df = pd.DataFrame(VALUES, columns=["time", "avg_delay"])

# Decimal > float (matplotlib не работает с Decimal)
df["avg_delay"] = df["avg_delay"].astype(float)

# построение
plt.figure(figsize=(8, 4))
plt.barh(df["time"], df["avg_delay"], height=0.03)

plt.xlabel("avg delay (minutes)")
plt.ylabel("time")
plt.tight_layout()
plt.show()

#########################################################
#########################################################

print('Job finished')    
    
