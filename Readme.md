# Belgian Railway Situation Monitoring Project  
*(based on the iRail API)*

## Description

This project performs continuous monitoring of the Belgian railway network using the iRail API.

Every hour, data for **all railway stations in Belgium** is collected and stored in a database. For each station, the following metrics are calculated on an hourly basis:

- **Average train delay** (in minutes)
- **Share of delayed trains**

Only delays greater than 2 minutes are taken into account.

As a result, the database contains, for every station and for every hour:
- the average delay value, and  
- the proportion of delayed trains.

In addition, an **online map** is generated every hour. Stations are visualized using different colors depending on the severity of delays. This allows:
- a quick assessment of the overall network situation, and  
- identification of problematic areas and bottlenecks.

A significant part of the project was devoted to 
determining the **real-time coordinates of moving trains**. 
However, the API providers silently removed this data and 
replaced it with zero values. With the current version of the API, 
this task is **fundamentally impossible** to solve.

## Database

The project uses a **MariaDB** database consisting of three tables connected via foreign keys:

- `stations`  
  Contains reference data for all railway stations.

- `update_runs`  
  Stores data about database update cycles (normally executed once per hour).

- `delays`  
  Contains delay metrics:
  - average delay in minutes (arithmetic mean of all delays > 2 minutes),
  - share of delayed trains (expressed in thousandths).

  This table references `stations` and `update_runs` via foreign keys.

The database is available in read-only mode via IP access. 
Detailed connection credentials can be obtained from the project developer.

## Online Map

Link to the continuously updated online map:

http://35.208.69.174/images/main_map.png

