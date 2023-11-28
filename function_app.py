import azure.functions as func
from azure.functions.decorators.core import DataType
import logging
import random
import pyodbc
import os

# pyodbc needs the SQL driver aswell as the SQL connection string for connecting.
sql_driver = "Driver={ODBC Driver 18 for SQL Server};"

# Get Azure Application Settings.
db_name = os.environ["DatabaseName"]
table_name = os.environ["TableName"]
sql_connection_string = sql_driver + os.environ["SqlConnectionString"]

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


# Azure function to simulate environment sensor readings, set to run every 5 seconds.
@app.function_name(name="generate_sensor_readings")
@app.timer_trigger(
    schedule="*/5 * * * * *",
    arg_name="timer",
    run_on_startup=True,
    use_monitor=False,
)
def generate_sensor_readings(timer: func.TimerRequest) -> None:
    if timer.past_due:
        logging.info("The timer is past due!")

    # Limits:
    # Temperature:          8 - 15
    # Wind Speed:           15 - 25
    # Relative Humidity:    40 - 70
    # CO2:                  500 - 1500

    # Generate readings.
    num_sensors = 20
    readings = list()
    for i in range(num_sensors):
        # NOTE: randrange is min inclusive, max exclusive (hence the +1)
        reading = {
            "sensor_id": i,
            "temp": random.randrange(8, 15 + 1),
            "wind_speed": random.randrange(15, 25 + 1),
            "rel_humidity": random.randrange(40, 70 + 1),
            "co2": random.randrange(500, 1500 + 1),
        }
        readings.append(reading)

    logging.info(f"Generated {len(readings)} readings.")

    # Save readings in database.
    conn = pyodbc.connect(sql_connection_string)
    cur = conn.cursor()

    # If the sensor readings table doesn't exist, create it.
    if not cur.tables(table=table_name, tableType="TABLE").fetchone():
        logging.info(f"Creating table {table_name}.")
        create_table_sql = f"CREATE TABLE {table_name}(id int IDENTITY(1,1) PRIMARY KEY, sensor_id int, temp int, wind_speed int, rel_humidity int, co2 int)"
        cur.execute(create_table_sql)
        conn.commit()

        # Enable change tracking on the table (this has to be it's own transaction)
        enable_tracking_sql = (
            f"ALTER TABLE [dbo].[{table_name}] ENABLE CHANGE_TRACKING;"
        )
        cur.execute(enable_tracking_sql)
        conn.commit()
        logging.info("Table created and tracking enabled.")

    else:
        logging.info(f"Table {table_name} exists.")

    # Insert the readings into the database.
    for r in readings:
        cur.execute(
            f"insert into {table_name}(sensor_id, temp, wind_speed, rel_humidity, co2) values (?,?,?,?,?)",
            r["sensor_id"],
            r["temp"],
            r["wind_speed"],
            r["rel_humidity"],
            r["co2"],
        )
    conn.commit()
    conn.close()


# Calculate statistics on the sensor data. Triggered by an update in the SensorReadings SQL table.
@app.function_name(name="analyse_sensor_readings")
@app.generic_trigger(
    arg_name="data",
    type="sqlTrigger",
    TableName=table_name,
    ConnectionStringSetting="SqlConnectionString",
    data_type=DataType.STRING,
)
def analyse_sensor_readings(data: str) -> None:
    # Connect to the database.
    conn = pyodbc.connect(sql_connection_string)
    cur = conn.cursor()

    # If the table doesn't exist, exit early.
    if not cur.tables(table=table_name, tableType="TABLE").fetchone():
        conn.close()
        logging.info("No sensor readings available.")
        return

    # Get the IDs of all the sensors in use.
    get_sensor_ids_sql = (
        f"SELECT DISTINCT sensor_id FROM {table_name} ORDER BY sensor_id ASC"
    )
    cur.execute(get_sensor_ids_sql)
    sensors_in_use = []
    for row in cur.fetchall():
        sensors_in_use.append(row[0])  # Store the sensor ID.

    stats = {}
    data_points = ["temp", "wind_speed", "co2", "rel_humidity"]
    for sensor in sensors_in_use:
        stats[sensor] = {}
        # Get min, max, average of each data point.
        for dat in data_points:
            # Make SQL calculate all the stats for us.
            get_stats_sql = f"SELECT MIN({dat}), MAX({dat}), AVG({dat}) FROM {table_name} WHERE (sensor_id = {sensor})"
            cur.execute(get_stats_sql)

            # Parse the results into a python dict.
            results = cur.fetchall()[0]
            data_point_stats = {
                "min": results[0],
                "max": results[1],
                "average": results[2],
            }
            stats[sensor][dat] = data_point_stats

    conn.commit()
    conn.close()

    logging.info("Stats Created.")
    logging.info(stats)
