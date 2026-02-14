# Goal: Analyze the CSV files to answer the main guiding question: What is the most common Chronic Disease that have been studied in MN (Minnesota)? Then, then questions for that most common Chronic Disease?
# Credit for the data extraction practice: https://catalog.data.gov/dataset/u-s-chronic-disease-indicators

import pandas as pd

# Extract the data from the CSV file
df = pd.read_csv("U.S._Chronic_Disease_Indicators.csv")
# print(df['Topic'])

# Transforming the data
# NOTE: Filter the data to only include the data that is related to MN State
mn_df = df[df['LocationDesc'] == "Minnesota"]
# print(mn_df.head())
# NOTE: Renaming the column for better clarity
mn_df.rename(columns = {"Question": "Questions Asked About the CD"}, inplace=True)
print(mn_df[["LocationDesc", "Questions Asked About the CD"]])
mn_df = mn_df[["LocationDesc", "Topic", "Questions Asked About the CD"]]

#Loading the data --> a new JSON file
mn_df.to_json("Minnesotan_Questions.json", orient="records", indent=4)
