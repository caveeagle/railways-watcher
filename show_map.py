import sys

import mariadb
import json
import pyproj
from PIL import Image, ImageDraw

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

if sys.platform.startswith("linux"):  # for my VM
    SQL_CONN_CONFIG['host'] = 'localhost'


##################################################################
##################################################################
##################################################################

SQL_REQUEST = '''
                SELECT
                    stations.lon AS lon,
                    stations.lat AS lat,
                    delays.avg_delay AS avg_delay,
                    delays.share_delayed AS share_delayed
                FROM delays
                JOIN stations
                  ON stations.ID = delays.station_ID
                WHERE delays.update_id = 5
                ORDER BY stations.station_id
'''

STATIONS_DATA = []

try:
    with mariadb.connect(**SQL_CONN_CONFIG) as conn:

        cur = conn.cursor(dictionary=True)

        cur.execute(SQL_REQUEST)

        rows = cur.fetchall()
        
        STATIONS_DATA.append((row['lon'], row['lat'], row['avg_delay'], row['share_delayed']))
        
except mariadb.Error as e:
    print(f"MariaDB error: {e}")

print(STATIONS_DATA)
raise SystemExit()

##################################################################
##################################################################
##################################################################

PNG_INPUT = "./images/base_belgium_map.png"
GEOREF_JSON = "./images/base_belgium_map.georef.json"

PNG_OUTPUT = "./images/show_stations.png"

##################################################################


img = Image.open(PNG_INPUT)
draw = ImageDraw.Draw(img)

RADIUS_PX = 2

#############

with open(GEOREF_JSON, "r", encoding="utf-8") as f:
    geo = json.load(f)

CRS = geo["crs"]
XMIN = geo["xmin"]
YMIN = geo["ymin"]
XMAX = geo["xmax"]
YMAX = geo["ymax"]
WIDTH_PX = geo["width_px"]
HEIGHT_PX = geo["height_px"]

# -------------------------------------
# COORDINATES > MAP CRS
# -------------------------------------

transformer = pyproj.Transformer.from_crs(
    "EPSG:4326",
    CRS,
    always_xy=True
)

for lon, lat in coords:

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
        fill='orange',
        outline=None
    )

if(1):
    img.save(PNG_OUTPUT)
    
##################################################################
##################################################################

print('\nJob finished!')

