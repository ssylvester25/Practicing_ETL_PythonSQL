# Goal: Changing the names of the row values and grouping the values (practicing SQL?)

import pandas as pd 
import sqlite3 as sql

# Extracting the data
df = pd.read_csv("U.S._Chronic_Disease_Indicators.csv")

# Transforming the data

# Objective: changing one of the row value names
df.loc[df['Question'] == 'Current asthma among adults', 'Question'] = "Adults who currently have asthma"
# Filter rows where the change was made
changed_df = df[df['Question'] == "Adults who currently have asthma"]
print(changed_df['Question'])

# Objective: grouping the values
grouped_values = df.groupby('LocationDesc')['Question'].count().reset_index()
grouped_values = grouped_values.rename(columns={"Question":"Number of Questions"}, inplace=False)
grouped_values = grouped_values.sort_values(by="Number of Questions", ascending=False)
# print(grouped_values.head())

# Observation: I only want to include state information, so we want to get rid of the data that only focuses on the country as a whole
grouped_values = grouped_values[grouped_values['LocationDesc'] != 'United States'] # Search through the grouped_values dataframe and for all of the values that ARE NOT 'United States', assign them into the dataframe. 
print(grouped_values.head())


# Load the data (grouped_values) into csv
grouped_values.to_csv("States_number_of_questions.csv", index=False)