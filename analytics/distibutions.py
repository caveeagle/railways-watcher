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

#VALUE_FIELD = 'avg_delay' 
VALUE_FIELD = 'share_delayed' 


SQL_QUERY = '''
            SELECT * FROM delays 
                WHERE update_id = 5  
                    AND avg_delay <> 0
            '''

#########################################################

try:
    with mariadb.connect(**SQL_CONN_CONFIG) as conn:

        cur = conn.cursor(dictionary=True)
        
        cur.execute(SQL_QUERY)

        rows = cur.fetchall()
        
        VALUES = [row[VALUE_FIELD] for row in rows]
        
except mariadb.Error as e:
    print(f'MariaDB error: {e}')
        
print(f'Name of field: {VALUE_FIELD}')
print(f'Number of values: {len(VALUES)}')
    
#########################################################
#########################################################

df = pd.DataFrame(VALUES, columns=['value'])

mean_value = df['value'].mean()
median_value = df['value'].median()
min_value = df['value'].min()
max_value = df['value'].max()

# Mode:
mode_value = df['value'].mode().iloc[0]

summary = {
    'mean': mean_value,
    'median': median_value,
    'min': min_value,
    'max': max_value,
    'mode': mode_value
}

print('Summary statistics:')
print(f'Mean: {mean_value:.2f}')
print(f'Median: {median_value}')
print(f'Minimum: {min_value}')
print(f'Maximum: {max_value}')
print(f'Mode (peak value): {mode_value}')

min_value_non_zero = df.loc[df['value'] > 0, 'value'].min()
print(f'Minimum (non-zero): {min_value_non_zero:.2f}')

#########################################################
#########################################################

if(1):
    
    N = 50
    
    max_value = 1000
    
    plt.figure(figsize=(8, 5))
    plt.hist(df['value'], bins=N)
    plt.xlim(0, max_value)
    plt.xlabel('value')
    plt.ylabel('Count')
    plt.title(f'{VALUE_FIELD} distribution ({N} bins)')
    plt.tight_layout()
    plt.show()

#########################################################
#########################################################

print('Job finished')    
    
