from flask import Flask, render_template, request, jsonify
import mysql.connector
import pandas as pd
from datetime import datetime, timedelta
import random
from pandas.api.types import is_numeric_dtype
app = Flask(__name__)
app.debug = True

@app.route('/')
def index():
    return render_template("index.html.jinja")

@app.route('/api/stations')
def api_stations():
    """
    Return JSON list of all station records (id, latitude, longitude, elevation, state, name).
    This endpoint is called by the front-end when the user clicks "Load Stations".
    """
    conn = mysql.connector.connect(
        host="localhost",
        port="3307",
        user="root",
        database="ustca",
        password=""
    )
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT id, latitude, longitude, elevation, state, name
        FROM station
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    """)
    stations = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify(stations) 

@app.route('/regional', methods=['GET', 'POST'])
def region():
    result = None
    tmax_slope = tmin_slope = prcp_slope = None
    mydb = mysql.connector.connect(
        host="localhost",
        port="3307",
        user="root",
        database="ustca",
        password=""
    )

    query = 'SELECT * FROM region_summary_stats'
    df = pd.read_sql(query, con=mydb)
    for col in df.columns:
        if is_numeric_dtype(df[col]):
            df[col] = df[col] / 10

    regions = df.to_dict(orient='records')
    
    cursor = mydb.cursor(dictionary=True)
    result = []
    submitted = False
    if request.method == 'POST':
        submitted = True 
        start_year = request.form.get('start_year')
        end_year = request.form.get('end_year')
        region = request.form.get('region')
        
        query = """
            SELECT 
                year, region,
                MAX(CASE WHEN element = 'TMIN' THEN avg_value END) AS tmin,
                MAX(CASE WHEN element = 'TMAX' THEN avg_value END) AS tmax,
                MAX(CASE WHEN element = 'PRCP' THEN avg_value END) AS prcp
            FROM regional_averages
            WHERE year BETWEEN %s AND %s
                AND region = %s
            GROUP BY year, region
        """

        cursor.execute(query, (start_year, end_year, region))
        result = cursor.fetchall()

        if result:
            years = [row['year'] for row in result if row['tmax'] is not None]
            tmax_values = [row['tmax'] for row in result if row['tmax'] is not None]

            n = len(years)
            if n > 1:
                x_mean = sum(years)/n
                y_mean = sum(tmax_values)/n
                numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(years, tmax_values))
                denominator = sum((x - x_mean) **2 for x in years)
                tmax_slope = numerator/denominator if denominator != 0 else None
            else:
                tmax_slope = None

            years = [row['year'] for row in result if row['tmin'] is not None]
            tmin_values = [row['tmin'] for row in result if row['tmin'] is not None]

            n = len(years)
            if n > 1:
                x_mean = sum(years)/n
                y_mean = sum(tmin_values)/n
                numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(years, tmin_values))
                denominator = sum((x - x_mean) **2 for x in years)
                tmin_slope = numerator/denominator if denominator != 0 else None
            else:
                tmin_slope = None
            
            years = [row['year'] for row in result if row['prcp'] is not None]
            prcp_values = [row['prcp'] for row in result if row['prcp'] is not None]
            n = len(years)
            if n > 1:
                x_mean = sum(years)/n
                y_mean = sum(prcp_values)/n
                numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(years, prcp_values))
                denominator = sum((x - x_mean) **2 for x in years)
                prcp_slope = numerator/denominator if denominator != 0 else None
            else:
                prcp_slope = None
    cursor.close()
    mydb.close()

    return render_template('regional.html.jinja', regions=regions, result=result, submitted=submitted, tmax_slope=tmax_slope, tmin_slope=tmin_slope, prcp_slope=prcp_slope)

@app.route('/seasonal', methods=['GET', 'POST'])
def season():
    result = None
    tmax_slope = tmin_slope = prcp_slope = None

    mydb = mysql.connector.connect(
    host="localhost",
    port="3307",
    user="root",
    database="ustca",
    password=""
    )    
    
    cursor = mydb.cursor(dictionary=True)
    result = []
    submitted = False
    if request.method == 'POST':
        submitted = True 
        start_year = request.form.get('start_year')
        end_year = request.form.get('end_year')
        quarter = request.form.get('quarter')
        quarter = f"Q{quarter}"
        state = request.form.get('state').upper()
        
        query = """
        SELECT 
            s.state, q.year, q.quarter,
            MAX(CASE WHEN q.element = 'TMIN' THEN q.average_value END) AS tmin,
            MAX(CASE WHEN q.element = 'TMAX' THEN q.average_value END) AS tmax,
            MAX(CASE WHEN q.element = 'PRCP' THEN q.average_value END) AS prcp
        FROM quarterly_averages q
        JOIN station s ON q.id = s.id
        WHERE q.year BETWEEN %s AND %s
            AND q.quarter = %s AND UPPER(s.state) = %s
        GROUP BY s.state, q.year, q.quarter
        ORDER BY q.year ASC
        """

        cursor.execute(query, (start_year, end_year, quarter, state))

        result = cursor.fetchall()
        print(result)

        if result:
            for row in result:
                if 'tmax' in row and row['tmax'] is not None:
                    row['tmax'] /= 10.0
                if 'tmin' in row and row['tmin'] is not None:
                    row['tmin'] /= 10.0
            years = [row['year'] for row in result if row['tmax'] is not None]
            tmax_values = [row['tmax'] for row in result if row['tmax'] is not None]

            n = len(years)
            if n > 1:
                x_mean = sum(years)/n
                y_mean = sum(tmax_values)/n
                numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(years, tmax_values))
                denominator = sum((x - x_mean) **2 for x in years)
                tmax_slope = numerator/denominator if denominator != 0 else None
            else:
                tmax_slope = None

            years = [row['year'] for row in result if row['tmin'] is not None]
            tmin_values = [row['tmin'] for row in result if row['tmin'] is not None]

            n = len(years)
            if n > 1:
                x_mean = sum(years)/n
                y_mean = sum(tmin_values)/n
                numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(years, tmin_values))
                denominator = sum((x - x_mean) **2 for x in years)
                tmin_slope = numerator/denominator if denominator != 0 else None
            else:
                tmin_slope = None
            
            years = [row['year'] for row in result if row['prcp'] is not None]
            prcp_values = [row['prcp'] for row in result if row['prcp'] is not None]
            n = len(years)
            if n > 1:
                x_mean = sum(years)/n
                y_mean = sum(prcp_values)/n
                numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(years, prcp_values))
                denominator = sum((x - x_mean) **2 for x in years)
                prcp_slope = numerator/denominator if denominator != 0 else None
            else:
                prcp_slope = None
        cursor.close()
        mydb.close()

    return render_template('seasonal.html.jinja', result = result, submitted=submitted, tmax_slope = tmax_slope, tmin_slope = tmin_slope, prcp_slope = prcp_slope)

@app.route('/elevation', methods=['GET', 'POST'])
def elevation():
    result = None
    tmax_slope = tmin_slope = prcp_slope = None

    mydb = mysql.connector.connect(
        host="localhost",
        port="3307",
        user="root",
        database="ustca",
        password=""
    )

    cursor = mydb.cursor(dictionary=True)
    result = []
    submitted = False
    if request.method == 'POST':
        submitted = True
        start_year = request.form.get('start_year')
        end_year = request.form.get('end_year')
        elevation = request.form.get('elevation_range')
        state = request.form.get('location').upper()

        if elevation == 'below_sea_level':
            elevation_query = "s.elevation < %s"
            elevation_params = (-1,)
        elif elevation == '0_150':
            elevation_query = "s.elevation BETWEEN %s AND %s"
            elevation_params = (0, 150)
        elif elevation == '150_300':
            elevation_query = "s.elevation BETWEEN %s AND %s"
            elevation_params = (150, 300)
        elif elevation == 'above_300':
            elevation_query = "s.elevation > %s"
            elevation_params = (300,)
        else:
            elevation_query = "1 = 0"
            elevation_params = ()

        query = f"""
            SELECT 
                s.state, YEAR(o.date) as year,
                AVG(o.tmin) AS tmin,
                AVG(o.tmax) AS tmax,
                AVG(o.prcp) AS prcp
            FROM observation o
            JOIN station s ON o.id = s.id
            WHERE YEAR(o.date) BETWEEN %s AND %s
              AND {elevation_query}
              AND UPPER(s.state) = %s
            GROUP BY s.state, YEAR(o.date)
            ORDER BY YEAR(o.date) ASC
        """

        params = (start_year, end_year, *elevation_params, state)
        cursor.execute(query, params)
        result = cursor.fetchall()

        if result:
            for row in result:
                if 'tmax' in row and row['tmax'] is not None:
                    row['tmax'] /= 10.0
                if 'tmin' in row and row['tmin'] is not None:
                    row['tmin'] /= 10.0
                if 'prcp' in row and row['prcp'] is not None:
                    row['prcp'] /= 10.0

            years = [row['year'] for row in result if row['tmax'] is not None]
            tmax_values = [row['tmax'] for row in result if row['tmax'] is not None]

            n = len(years)
            if n > 1:
                x_mean = sum(years) / n
                y_mean = sum(tmax_values) / n
                numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(years, tmax_values))
                denominator = sum((x - x_mean) ** 2 for x in years)
                tmax_slope = numerator / denominator if denominator != 0 else None
            else:
                tmax_slope = None

            years = [row['year'] for row in result if row['tmin'] is not None]
            tmin_values = [row['tmin'] for row in result if row['tmin'] is not None]

            n = len(years)
            if n > 1:
                x_mean = sum(years) / n
                y_mean = sum(tmin_values) / n
                numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(years, tmin_values))
                denominator = sum((x - x_mean) ** 2 for x in years)
                tmin_slope = numerator / denominator if denominator != 0 else None
            else:
                tmin_slope = None

            years = [row['year'] for row in result if row['prcp'] is not None]
            prcp_values = [row['prcp'] for row in result if row['prcp'] is not None]

            n = len(years)
            if n > 1:
                x_mean = sum(years) / n
                y_mean = sum(prcp_values) / n
                numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(years, prcp_values))
                denominator = sum((x - x_mean) ** 2 for x in years)
                prcp_slope = numerator / denominator if denominator != 0 else None
            else:
                prcp_slope = None

        cursor.close()
        mydb.close()
    return render_template('elevation.html.jinja', result=result, submitted=submitted, tmax_slope=tmax_slope, tmin_slope=tmin_slope, prcp_slope=prcp_slope)

@app.route('/climate', methods=['GET', 'POST'])
def climate():
    result = None
    slope = None
    metric = None 

    mydb = mysql.connector.connect(
        host="localhost",
        port="3307",
        user="root",
        database="ustca",
        password=""
    )
    cursor = mydb.cursor(dictionary=True)
    submitted = False

    if request.method == 'POST':
        submitted = True
        start_year = request.form.get('start_year')
        end_year = request.form.get('end_year')
        climate_class = request.form.get('climate_class')
        metric = request.form.get('metric').lower()
        
        query = f"""
            SELECT 
                YEAR(o.date) AS year,
                AVG(o.{metric}) AS value
            FROM observation o
            JOIN climate c ON o.id = c.id
            WHERE YEAR(o.date) BETWEEN %s AND %s
            AND c.class = %s
            GROUP BY YEAR(o.date)
            ORDER BY year ASC
        """

        cursor.execute(query, (start_year, end_year, climate_class))
        result = cursor.fetchall()
        
        if metric in ['tmax', 'tmin']:
            for row in result:
                if row['value'] is not None:
                    row['value'] = row['value'] / 10.0
        
        if result:
            years = [row['year'] for row in result if row['value'] is not None]
            values = [row['value'] for row in result if row['value'] is not None]

            n = len(years)
            if n > 1:
                x_mean = sum(years) / n
                y_mean = sum(values) / n
                numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(years, values))
                denominator = sum((x - x_mean) ** 2 for x in years)
                slope = numerator / denominator if denominator != 0 else None
            else:
                slope = None

    cursor.close()
    mydb.close()

    return render_template('climate.html.jinja', result=result, submitted=submitted, slope=slope, metric=metric)
    
if __name__ == "__main__":
    app.run(port=5001, debug=True)

