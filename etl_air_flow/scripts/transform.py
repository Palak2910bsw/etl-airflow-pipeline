import requests
import pandas as pd
import psycopg2

def run_etl():
    # Fetch COVID data by country
    response = requests.get("https://disease.sh/v3/covid-19/countries")
    data = response.json()

    # Convert to DataFrame
    df = pd.DataFrame(data)

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host="postgres",
        database="sales",  # or your DB name
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    # Create table if not exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS covid_stats (
            country TEXT PRIMARY KEY,
            cases BIGINT,
            deaths BIGINT,
            recovered BIGINT,
            active BIGINT,
            tests BIGINT
        );
    """)
    conn.commit()

    # Insert or update rows
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO covid_stats (country, cases, deaths, recovered, active, tests)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (country) DO UPDATE SET
                cases = EXCLUDED.cases,
                deaths = EXCLUDED.deaths,
                recovered = EXCLUDED.recovered,
                active = EXCLUDED.active,
                tests = EXCLUDED.tests;
        """, (
            row['country'],
            row['cases'],
            row['deaths'],
            row['recovered'],
            row['active'],
            row['tests']
        ))

    conn.commit()
    cur.close()
    conn.close()

    print("COVID stats data loaded successfully.")
