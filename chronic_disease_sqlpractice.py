import pandas as pd
import sqlite3

# Extracting the data
df = pd.read_csv("U.S._Chronic_Disease_Indicators.csv")

# Creating the connection to the SQL Database
conn = sqlite3.connect("my_database.db")

# Putting the data from the CSV file into the SQL Database
df.to_sql("chronic_diseases", conn, index=False, if_exists="replace")

# SQL Queries 
# This query groups the questions that have the same string value and counts the number of rows within the group
query = """
SELECT LocationDesc, Question, COUNT(*) AS Number_of_questions
FROM chronic_diseases
WHERE LocationDesc = "Texas"
GROUP BY Question
ORDER BY Number_of_questions DESC
"""
tx_data = pd.read_sql(query, conn)
print(tx_data)

# Load
tx_data.to_csv("Texas_questions.csv", index=False)