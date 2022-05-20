import sqlite3
import os
import pandas as pd

# if test.db exists, delete it
if os.path.exists("test.db"):
    os.remove("test.db")

connection = sqlite3.connect("test.db")
cursor = connection.cursor()



cursor.execute(
    "CREATE TABLE chapters (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, description TEXT NOT NULL, code TEXT NOT NULL)")
cursor.execute(
    "CREATE TABLE headings (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, description TEXT NOT NULL, code TEXT NOT NULL, chapter TEXT NOT NULL, FOREIGN KEY (chapter) REFERENCES chapters (id))")
cursor.execute(
    "CREATE TABLE subheadings (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, description TEXT NOT NULL, code TEXT NOT NULL, heading TEXT NOT NULL, FOREIGN KEY (heading) REFERENCES headings (id))")
cursor.execute(
    "CREATE TABLE country_wise (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, description TEXT NOT NULL, code TEXT NOT NULL, country TEXT NOT NULL, subheading TEXT NOT NULL, FOREIGN KEY (subheading) REFERENCES subheadings (id))")


chapters = pd.read_csv("chapters.csv")
headings = pd.read_csv("headings.csv")
subheadings = pd.read_csv("subheadings.csv")
country_wise = pd.read_csv("country_wise.csv")



for rows in chapters.itertuples():
    code, description = rows[1], rows[2]
    code, description = str(code), str(description)
    cursor.execute("INSERT INTO chapters (description, code) VALUES (?, ?)", (description, code))

connection.commit()


for rows in headings.itertuples():
    code, description = rows[1], rows[2]
    code, description = str(code), str(description)
    
    chapter_code = code[:2]
    cursor.execute("SELECT id FROM chapters WHERE code = ?", (chapter_code,))
    chapter_id = cursor.fetchone()[0]

    cursor.execute("INSERT INTO headings (description, code, chapter) VALUES (?, ?, ?)", (description, code, chapter_id))

connection.commit()


for rows in subheadings.itertuples():
    code, description = rows[1], rows[2]
    code, description = str(code), str(description)

    heading_code = code[:4]
    cursor.execute("SELECT id FROM headings WHERE code = ?", (heading_code,))
    heading_id = cursor.fetchone()[0]

    cursor.execute("INSERT INTO subheadings (description, code, heading) VALUES (?, ?, ?)", (description, code, heading_id))

connection.commit()


for rows in country_wise.itertuples():
    code, description = rows[1], rows[2]
    code, description = str(code), str(description)

    subheading_code = code[:6]
    cursor.execute("SELECT id FROM subheadings WHERE code = ?", (subheading_code,))
    subheading_id = cursor.fetchone()[0]

    cursor.execute("INSERT INTO country_wise (description, code, country, subheading) VALUES (?, ?, ?, ?)", (description, code, "USA", subheading_id))


connection.commit()
