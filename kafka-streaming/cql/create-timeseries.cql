/*
Schema for storing raw ISD-lite hourly weather data.
More can be read about these weather sets here: http://www.ncdc.noaa.gov/oa/climate/isd/
*/

DROP KEYSPACE IF EXISTS isd_weather_data;
CREATE KEYSPACE isd_weather_data WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

use isd_weather_data;

/*
Raw weather readings from a single station, hourly.
sky_condition_text text, // Non-coded sky conditions
*/

CREATE TABLE raw_weather_data (
   wsid text,               // Composite of Air Force Datsav3 station number and NCDC WBAN number
   year int,                // Year collected
   month int,               // Month collected
   day int,                 // Day collected
   hour int,                // Hour collected
   temperature double,       // Air temperature (degrees Celsius)
   dewpoint double,          // Dew point temperature (degrees Celsius)
   pressure double,          // Sea level pressure (hectopascals)
   wind_direction int,      // Wind direction in degrees. 0-359
   wind_speed double,        // Wind speed (meters per second)
   sky_condition int,       // Total cloud cover (coded, see format documentation)
   sky_condition_text text, // Non-coded sky conditions
   one_hour_precip double,   // One-hour accumulated liquid precipitation (millimeters)
   six_hour_precip double,   // Six-hour accumulated liquid precipitation (millimeters)
   PRIMARY KEY ((wsid), year, month, day, hour)
) WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC, hour DESC);

/*
  Sum of all one_hour_precip for one day and one weather station
*/
CREATE TABLE daily_aggregate_precip (
   wsid text,
   year int,
   month int,
   day int,
   precipitation counter,
   PRIMARY KEY ((wsid), year, month, day)
) WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC);
