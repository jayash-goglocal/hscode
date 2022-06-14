import psycopg2

import pandas as pd

import os

# os.environ["DATABASE_URL"] = "localhost"
# os.environ["POSTGRES_USER"] = "postgres"
# os.environ["POSTGRES_PASSWORD"] = "postgres"
# os.environ["POSTGRES_DB_NAME"] = "hscodes"

os.environ["DATABASE_URL"] = "seller-central.cuybhpblpref.ap-south-1.rds.amazonaws.com"
os.environ["POSTGRES_USER"] = "postgres"
os.environ["POSTGRES_PASSWORD"] = "goGlocal123"
os.environ["POSTGRES_DB_NAME"] = "hscodes"


conn = psycopg2.connect(
    dbname = os.getenv("POSTGRES_DB_NAME"),
    host = os.getenv("DATABASE_URL"),
    user = os.getenv("POSTGRES_USER"),
    password = os.getenv("POSTGRES_PASSWORD")
)
conn.autocommit = True
cursor = conn.cursor()




cursor.execute(
    "CREATE TABLE chapters (id BIGSERIAL PRIMARY KEY NOT NULL, description TEXT NOT NULL, code TEXT NOT NULL)")
cursor.execute(
    "CREATE TABLE headings (id BIGSERIAL PRIMARY KEY NOT NULL, description TEXT NOT NULL, code TEXT NOT NULL, chapter BIGSERIAL REFERENCES chapters(id) ON DELETE CASCADE)")
cursor.execute(
    "CREATE TABLE subheadings (id BIGSERIAL PRIMARY KEY NOT NULL, description TEXT NOT NULL, code TEXT NOT NULL, heading BIGSERIAL REFERENCES headings (id) ON DELETE CASCADE)")
cursor.execute(
    "CREATE TABLE country_wise (id BIGSERIAL PRIMARY KEY NOT NULL, description TEXT NOT NULL, code TEXT NOT NULL, country TEXT NOT NULL, subheading BIGSERIAL REFERENCES subheadings (id) ON DELETE CASCADE)")
cursor.execute(
    "CREATE TABLE custom_duty (id BIGSERIAL PRIMARY KEY NOT NULL, hscode TEXT NOT NULL, country TEXT NOT NULL)")
cursor.execute(
    "CREATE TABLE sales_tax (id BIGSERIAL PRIMARY KEY NOT NULL, hscode TEXT NOT NULL, country TEXT NOT NULL)")
cursor.execute(
    "CREATE TABLE vat (id BIGSERIAL PRIMARY KEY NOT NULL, hscode TEXT NOT NULL, country TEXT NOT NULL)")



chapters = pd.read_csv("chapters.csv")
headings = pd.read_csv("headings.csv")
subheadings = pd.read_csv("subheadings.csv")
country_wise = pd.read_csv("country_wise.csv")



for rows in chapters.itertuples():
    code, description = rows[1], rows[2]
    code, description = str(code), str(description)
    cursor.execute("INSERT INTO chapters (description, code) VALUES (%s, %s)", (description, code))

conn.commit()


for rows in headings.itertuples():
    code, description = rows[1], rows[2]
    code, description = str(code), str(description)
    
    chapter_code = code[:2]
    cursor.execute("SELECT id FROM chapters WHERE code = %s", (chapter_code,))
    chapter_id = cursor.fetchone()[0]

    cursor.execute("INSERT INTO headings (description, code, chapter) VALUES (%s, %s, %s)", (description, code, chapter_id))

conn.commit()


for rows in subheadings.itertuples():
    code, description = rows[1], rows[2]
    code, description = str(code), str(description)

    heading_code = code[:4]
    cursor.execute("SELECT id FROM headings WHERE code = %s", (heading_code,))
    heading_id = cursor.fetchone()[0]

    cursor.execute("INSERT INTO subheadings (description, code, heading) VALUES (%s, %s, %s)", (description, code, heading_id))

conn.commit()


for rows in country_wise.itertuples():
    code, description = rows[1], rows[2]
    code, description = str(code), str(description)

    subheading_code = code[:6]
    cursor.execute("SELECT id FROM subheadings WHERE code = %s", (subheading_code,))
    subheading_id = cursor.fetchone()[0]

    cursor.execute("INSERT INTO country_wise (description, code, country, subheading) VALUES (%s, %s, %s, %s)", (description, code, "USA", subheading_id))


conn.commit()
