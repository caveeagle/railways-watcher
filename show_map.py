import sys
import os
import time

import mariadb
import json
import pyproj
from PIL import Image, ImageDraw, ImageFont

import config
import config_secrets

##################################################################

# Change to your own parameters #

SQL_CONN_CONFIG = {
    'user': config_secrets.READ_USER,
    'password': config_secrets.READ_USER_PWD,
    'host': config_secrets.SERVER_IP,
    'database': config_secrets.DB_NAME,
    'port': 3306 # by default
}

if sys.platform.startswith('linux'):  # for my VM
    SQL_CONN_CONFIG['host'] = 'localhost'

##################################################################
##################################################################
##################################################################

LAST_UPDATE_ID = 0

try:
    with mariadb.connect(**SQL_CONN_CONFIG) as conn:

        cursor = conn.cursor()

        sql_req = '''
                    SELECT *
                    FROM update_runs
                    ORDER BY update_time DESC
                    LIMIT 1;
        '''
        cursor.execute(sql_req)

        row = cursor.fetchone()
        
        LAST_UPDATE_ID = row[0]
        last_update_time = row[1]
        
        print(f'Last update at {last_update_time} with update_id {LAST_UPDATE_ID}')
        
except mariadb.Error as e:
    print(f'MariaDB error: {e}')

##################################################################
##################################################################
##################################################################

SQL_REQUEST = f'''
                SELECT
                    stations.lon AS lon,
                    stations.lat AS lat,
                    delays.avg_delay AS avg_delay,
                    delays.share_delayed AS share_delayed
                FROM delays
                JOIN stations
                  ON stations.ID = delays.station_ID
                WHERE delays.update_id = {LAST_UPDATE_ID}
                ORDER BY stations.station_id
'''

STATIONS_DATA = []

try:
    with mariadb.connect(**SQL_CONN_CONFIG) as conn:

        cur = conn.cursor(dictionary=True)

        cur.execute(SQL_REQUEST)

        rows = cur.fetchall()
        
        STATIONS_DATA = [(row['lon'], row['lat'], row['avg_delay'], row['share_delayed']) for row in rows]
        
except mariadb.Error as e:
    print(f'MariaDB error: {e}')

##################################################################
##################################################################
##################################################################

def get_station_status(avg_delay,share_delayed):
    
    if ( avg_delay == 0 ):
        return 0  # No delays, code green

    if ( avg_delay < 10 ):
        return 1  # Light delays, code yellow
    
    #############################
    
    if( (10 <= avg_delay < 15)  and (share_delayed > 450) ) :
        return 2  # Middle, code red

    if( (10 <= avg_delay < 15)  and (share_delayed <= 450) ) :
        return 1  # Light

    if( (avg_delay >= 15) and (share_delayed <= 450) ):
        return 2  # Middle

    if( (avg_delay >= 15) and (share_delayed > 450) ):
        return 3  # Severe , code orange

    assert False  # wrong argumnts or conditions
     
##################################################################
##################################################################
##################################################################

PNG_INPUT_FILE   = 'images/base_belgium_map.png'
GEOREF_JSON_FILE = 'images/base_belgium_map.georef.json'
PNG_OUTPUT_FILE  = 'images/main_map.png'

if os.path.isfile('./'+PNG_INPUT_FILE):
    FULL_PATH = './'     
else:
    # In case of CRONTAB executive - need an abs. path
    FULL_PATH = config_secrets.PROJECT_PATH+'/'    
    
PNG_INPUT = FULL_PATH+PNG_INPUT_FILE
GEOREF_JSON = FULL_PATH+GEOREF_JSON_FILE
PNG_OUTPUT = FULL_PATH+PNG_OUTPUT_FILE

##################################################################

img = Image.open(PNG_INPUT)
draw = ImageDraw.Draw(img)

#############

with open(GEOREF_JSON, 'r', encoding='utf-8') as f:
    geo = json.load(f)

CRS = geo['crs']
XMIN = geo['xmin']
YMIN = geo['ymin']
XMAX = geo['xmax']
YMAX = geo['ymax']
WIDTH_PX = geo['width_px']
HEIGHT_PX = geo['height_px']

# -------------------------------------
# COORDINATES > MAP CRS
# -------------------------------------

transformer = pyproj.Transformer.from_crs(
    'EPSG:4326',
    CRS,
    always_xy=True
)

for (lon, lat, avg_delay, share_delayed) in STATIONS_DATA:
    
    status = get_station_status(avg_delay,share_delayed)  # 0,1,2,3
    
    # default value:
    if(status == 0):
        RADIUS_PX = 2
        COLOR = 'orange'
    
    if(status == 1):
        RADIUS_PX = 4
        COLOR = 'green'

    if(status == 2):
        RADIUS_PX = 4
        COLOR = 'red'

    if(status >= 3):
        RADIUS_PX = 8
        COLOR = 'red'
    
    
    x, y = transformer.transform(lon, lat)
    
    px = int((x - XMIN) / (XMAX - XMIN) * WIDTH_PX)
    py = int((YMAX - y) / (YMAX - YMIN) * HEIGHT_PX)  # Y-axis inverted

    draw.ellipse(
        (
            px - RADIUS_PX,
            py - RADIUS_PX,
            px + RADIUS_PX,
            py + RADIUS_PX
        ),
        fill=COLOR,
        outline=None
    )

###############################################

font_paths = [
    '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf', # Debian / Linux
    'C:/Windows/Fonts/times.ttf'                       # Windows
]

for path in font_paths:
    if os.path.exists(path):
        font = ImageFont.truetype(path, 80)
        break
else:
    font = ImageFont.load_default()

now = time.strftime("%d.%m.%Y - %H:%M", time.localtime())
    
text = 'Update at: '+now

position = (50, 50)          # x, y
color = (255, 0, 0)          # RGB

draw.text(
    (50, 45),
    text,
    fill=(255, 0, 0),
    font=font
) 

###############################################

    
WRITE_FILE = 1

if(WRITE_FILE):
    img.save(PNG_OUTPUT)
else:
    img.show()    
    
##################################################################
##################################################################

print('\nJob finished!')

