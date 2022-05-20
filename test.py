import sqlite3

connection = sqlite3.connect("test.db")
cursor = connection.cursor()


code = "6006310080"

data = cursor.execute("SELECT * FROM country_wise WHERE code = ?", (code,)).fetchall()


subheading_id = data[0][4]

subheading = cursor.execute("SELECT * FROM subheadings WHERE id = ?", (subheading_id,)).fetchall()

heading_id = subheading[0][3]

heading = cursor.execute("SELECT * FROM headings WHERE id = ?", (heading_id,)).fetchall()


print(data)
print(subheading)
print(heading)

# custom_duty
# sales_tax