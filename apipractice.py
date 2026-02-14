# Credit for this API data extraction practice: https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2020-to-Present/erm2-nwe9/about_data

import pandas as pd 

# Extract
# Finding the API URL with the data
url = "https://data.cityofnewyork.us/resource/erm2-nwe9.json?$query=SELECT%0A%20%20%60unique_key%60%2C%0A%20%20%60created_date%60%2C%0A%20%20%60closed_date%60%2C%0A%20%20%60agency%60%2C%0A%20%20%60agency_name%60%2C%0A%20%20%60complaint_type%60%2C%0A%20%20%60descriptor%60%2C%0A%20%20%60descriptor_2%60%2C%0A%20%20%60location_type%60%2C%0A%20%20%60incident_zip%60%2C%0A%20%20%60incident_address%60%2C%0A%20%20%60street_name%60%2C%0A%20%20%60cross_street_1%60%2C%0A%20%20%60cross_street_2%60%2C%0A%20%20%60intersection_street_1%60%2C%0A%20%20%60intersection_street_2%60%2C%0A%20%20%60address_type%60%2C%0A%20%20%60city%60%2C%0A%20%20%60landmark%60%2C%0A%20%20%60facility_type%60%2C%0A%20%20%60status%60%2C%0A%20%20%60due_date%60%2C%0A%20%20%60resolution_description%60%2C%0A%20%20%60resolution_action_updated_date%60%2C%0A%20%20%60community_board%60%2C%0A%20%20%60council_district%60%2C%0A%20%20%60police_precinct%60%2C%0A%20%20%60bbl%60%2C%0A%20%20%60borough%60%2C%0A%20%20%60x_coordinate_state_plane%60%2C%0A%20%20%60y_coordinate_state_plane%60%2C%0A%20%20%60open_data_channel_type%60%2C%0A%20%20%60park_facility_name%60%2C%0A%20%20%60park_borough%60%2C%0A%20%20%60vehicle_type%60%2C%0A%20%20%60taxi_company_borough%60%2C%0A%20%20%60taxi_pick_up_location%60%2C%0A%20%20%60bridge_highway_name%60%2C%0A%20%20%60bridge_highway_direction%60%2C%0A%20%20%60road_ramp%60%2C%0A%20%20%60bridge_highway_segment%60%2C%0A%20%20%60latitude%60%2C%0A%20%20%60longitude%60%2C%0A%20%20%60location%60%0AORDER%20BY%20%60created_date%60%20DESC%20NULL%20FIRST"

# Reading the JSON file that contains the API data (using pandas)
df = pd.read_json(url)

# Transform
# Choosing the exact location / index to gather our data (all rows, only columns 1-2, the end is excluded) 
df_transformed = df.iloc[:, 1:3]
# Storing only the values from the two columns into df_transformed
df_transformed = df[['created_date', 'closed_date']]
# Changing the names of the columns for greater clarity / transforming what is written in the table
df_transformed.columns = ['CreatedDate', 'ClosedDate']

# Convert date columns to datetime
df_transformed['CreatedDate'] = pd.to_datetime(df_transformed['CreatedDate'], errors='coerce') # Change the dates to include the date and time. If there are errors, don't crash --> Change it to NaT (or NaN)
df_transformed['ClosedDate'] = pd.to_datetime(df_transformed['ClosedDate'], errors='coerce')

# Filter rows where ClosedDate is not null
df_transformed = df_transformed[df_transformed['ClosedDate'].notna()]

print("Transformed data shape:", df_transformed.shape)
print(df_transformed.head())
# print(df.shape)



# Load 
# df_transformed.to_csv("nyc_311_data.csv", index=False)

# Or save as Parquet (recommended for big pipelines)
df_transformed.to_parquet("nyc_311_data.parquet", index=False)

print("Data loaded successfully!")