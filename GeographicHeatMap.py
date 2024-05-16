#Alejandro Urbano and Jonathon Dooley 
#!/usr/bin/env python
# coding: utf-8

# In[9]:


get_ipython().system('pip install requests boto3')
get_ipython().system('pip install xmltodict')


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[26]:


import requests
import boto3
import json
import xmltodict

# Define the start and end dates
start_date = "2023-01-01T00:00:00"
end_date = "2024-05-15T23:59:59"

# Define the API URL with parameters for the date range and minimum magnitude
url = f"https://service.iris.edu/fdsnws/event/1/query?starttime={start_date}&endtime={end_date}&minmag=5.0&format=xml"

# Fetch the data from the API
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Convert the XML response content to JSON
    xml_data = response.text
    data = xmltodict.parse(xml_data)
    events = data.get("q:quakeml", {}).get("eventParameters", {}).get("event", [])

    # Print the entire parsed data for debugging purposes
#    print(json.dumps(data, indent=4))

    def extract_event_details(event):
        return {
            "author": event.get('origin', {}).get('creationInfo', {}).get('author', 'N/A'),
            "latitude": event.get('origin', {}).get('latitude', {}).get('value', 'N/A'),
            "longitude": event.get('origin', {}).get('longitude', {}).get('value', 'N/A'),
            "time": event.get('origin', {}).get('time', {}).get('value', 'N/A'),
            "iris_contributor": event.get('origin', {}).get('@iris:contributor', 'N/A'),
            "publicID": event.get('@publicID', '').split('eventid=')[-1],
            "depth": event.get('origin', {}).get('depth', {}).get('value', 'N/A')
        }

    s3 = boto3.client('s3')
    bucket_name = '4540final'

    if events:
        for event in events:
            event_details = extract_event_details(event)
            public_id = event_details["publicID"]
            key = f'{public_id}.json'

            # Overwrite the object in S3
            s3.put_object(Bucket=bucket_name, Key=key, Body=json.dumps(event_details, indent=4))
            print(f"Event {public_id} saved to S3 successfully!")
else:
    print("Failed to retrieve data: Status code", response.status_code)
    print("Response content:", response.text)


# In[ ]:





# In[ ]:





# In[ ]:





# In[34]:


import requests
import boto3
import json
from botocore.exceptions import ClientError

# Define the start and end dates
start_date = "2023-01-01"
end_date = "2024-05-15"

# Define the API URL with parameters for the date range and minimum magnitude
url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_date}&endtime={end_date}&minmagnitude=5.0"

# Fetch the data from the API
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Convert the response content to JSON
    data = response.json()
    events = data.get("features", [])

    def extract_event_details(event):
        properties = event.get('properties', {})
        geometry = event.get('geometry', {})
        coordinates = geometry.get('coordinates', [None, None, None])
        return {
            "author": properties.get('net', 'N/A'),
            "latitude": coordinates[1],
            "longitude": coordinates[0],
            "time": properties.get('time', 'N/A'),
            "iris_contributor": properties.get('net', 'N/A'),
            "publicID": event.get('id', 'N/A'),
            "depth": coordinates[2],
            "magnitude": properties.get('mag', 'N/A'),
            "tsunami": properties.get('tsunami', 'N/A')
        }

    s3 = boto3.client('s3')
    bucket_name = '4540final'  # Replace with your S3 bucket name
    count = 0

    if events:
        for event in events:  # Process all events
            event_details = extract_event_details(event)
            public_id = event_details["publicID"]
            key = f'{public_id}.json'
            count += 1
            if count % 100 == 0:
                print(f"Processed {count} events")

            try:
                # Save the object to S3
                s3.put_object(Bucket=bucket_name, Key=key, Body=json.dumps(event_details, indent=4))
               # print(f"Event {public_id} saved to S3 successfully!")
            except ClientError as e:
                print(f"Failed to upload {public_id} to S3: {e}")
else:
    print("Failed to retrieve data: Status code", response.status_code)
    #print("Response content:", response.text)


# In[ ]:





# In[ ]:





# In[ ]:


from datetime import datetime, timedelta

def remove_near_duplicates(locations, time_threshold_seconds=60, coord_precision=5):
    """
    Remove near-duplicate locations based on latitude, longitude, and time.

    Args:
    locations (list of dict): List of location dictionaries with 'latitude', 'longitude', and 'time' keys.
    time_threshold_seconds (int): Time threshold in seconds to consider locations as duplicates.
    coord_precision (int): Precision for rounding coordinates to identify near-duplicates.

    Returns:
    list of dict: List of unique locations.
    """
    unique_locations = []
    seen = set()

    for loc in locations:
        lat = round(loc['latitude'], coord_precision)
        lon = round(loc['longitude'], coord_precision)
        time = datetime.utcfromtimestamp(loc['time'] / 1000)  # Convert from milliseconds to datetime

        # Generate a unique key based on rounded coordinates and time within the threshold
        key = (lat, lon)
        duplicate_found = False

        for seen_time in seen:
            if (key in seen) and abs((time - seen_time).total_seconds()) <= time_threshold_seconds:
                duplicate_found = True
                break

        if not duplicate_found:
            unique_locations.append(loc)
            seen.add(time)

    return unique_locations

# Example usage with the 'locations' list
# Each location should be a dictionary with 'latitude', 'longitude', and 'time' keys
locations = [
    {"latitude": 34.0522, "longitude": -118.2437, "time": 1684351200000},
    {"latitude": 34.0522, "longitude": -118.2437, "time": 1684351205000},  # Duplicate within threshold
    {"latitude": 40.7128, "longitude": -74.0060, "time": 1684351210000},
    {"latitude": 34.0522, "longitude": -118.2437, "time": 1684351220000},  # Not a duplicate
]

# Call the function to remove near-duplicates
unique_locations = remove_near_duplicates(locations)


# In[ ]:





# In[ ]:





# In[35]:


get_ipython().system('pip install folium')


# In[36]:


import boto3
import json
import folium
from folium.plugins import HeatMap

# Initialize S3 client
s3 = boto3.client('s3')

# Replace with your S3 bucket name and prefix (if any)
bucket_name = '4540final'
prefix = '/'

# List all JSON objects in the S3 bucket
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
json_objects = response.get('Contents', [])

# Extract data from JSON objects
locations = []
for obj in json_objects:
    obj_key = obj['Key']
    obj_body = s3.get_object(Bucket=bucket_name, Key=obj_key)['Body'].read()
    data = json.loads(obj_body)
    # Assuming the JSON has 'latitude' and 'longitude' keys
    latitude = data.get('latitude')
    longitude = data.get('longitude')
    if latitude and longitude:
        locations.append([latitude, longitude])

# Check the collected locations
print(f"Collected {len(locations)} locations")

### Step 3: Create a Heatmap

# Create a base map
m = folium.Map(location=[0, 0], zoom_start=2)

# Add heatmap layer
HeatMap(locations).add_to(m)

# Save the map to an HTML file
m.save('heatmap.html')


# In[38]:


import boto3
import json
import folium
from folium.plugins import HeatMap
from IPython.display import display, IFrame

# Initialize S3 client
s3 = boto3.client('s3')

# Replace with your S3 bucket name
bucket_name = '4540final'

# List all JSON objects in the S3 bucket root
response = s3.list_objects_v2(Bucket=bucket_name)
json_objects = response.get('Contents', [])

# Extract data from JSON objects
locations = []
for obj in json_objects:
    obj_key = obj['Key']
    if obj_key.endswith('.json'):  # Ensure the file is a JSON file
        obj_body = s3.get_object(Bucket=bucket_name, Key=obj_key)['Body'].read()
        data = json.loads(obj_body)
        # Assuming the JSON has 'latitude' and 'longitude' keys
        latitude = data.get('latitude')
        longitude = data.get('longitude')
        if latitude and longitude:
            locations.append([latitude, longitude])

# Check the collected locations
print(f"Collected {len(locations)} locations")

# Create a base map
m = folium.Map(location=[0, 0], zoom_start=2)

# Add heatmap layer
HeatMap(locations).add_to(m)

# Save the map to an HTML file
heatmap_path = 'heatmap.html'
m.save(heatmap_path)

# Display the heatmap within the notebook
display(IFrame(src=heatmap_path, width=700, height=500))


# In[ ]:





# In[51]:


import boto3
import json
import folium
from folium.plugins import HeatMap
from IPython.display import display, IFrame
from statistics import mean

# Initialize S3 client
s3 = boto3.client('s3')

# Replace with your S3 bucket name
bucket_name = '4540final'

# List all JSON objects in the S3 bucket root
response = s3.list_objects_v2(Bucket=bucket_name)
json_objects = response.get('Contents', [])

# Extract data from JSON objects
locations = []
tsunami_locations = []
for obj in json_objects:
    obj_key = obj['Key']
    if obj_key.endswith('.json'):  # Ensure the file is a JSON file
        obj_body = s3.get_object(Bucket=bucket_name, Key=obj_key)['Body'].read()
        data = json.loads(obj_body)
        # Assuming the JSON has 'latitude', 'longitude', and 'tsunami' keys
        try:
            latitude = float(data.get('latitude'))
            longitude = float(data.get('longitude'))
            tsunami = int(data.get('tsunami', 0))
            if latitude is not None and longitude is not None:
                locations.append([latitude, longitude])
                if tsunami != 0:
                    tsunami_locations.append([latitude, longitude])
        except (TypeError, ValueError) as e:
            print(f"Error processing data: {data}, error: {e}")

# Calculate the center of the locations for the map
if locations:
    avg_lat = mean([loc[0] for loc in locations])
    avg_lon = mean([loc[1] for loc in locations])
    center_location = [avg_lat, avg_lon]
else:
    center_location = [0, 0]  # Default to center of the world if no locations

# Create a base map centered on the average location
m = folium.Map(location=center_location, zoom_start=2)

# Add heatmap layer
HeatMap(locations).add_to(m)

# Add distinct markers for tsunami locations
for loc in tsunami_locations:
    folium.CircleMarker(
        location=loc,
        radius=5,
        color='red',
        fill=True,
        fill_color='red'
    ).add_to(m)

# Define the HTML for the color legend
legend_html = '''
<div style="position: fixed; 
            bottom: 50px; left: 50px; width: 150px; height: 180px; 
            background-color: white; border:2px solid grey; z-index:9999; font-size:14px;
            ">
&emsp;<strong>Heatmap Legend</strong><br>
&emsp;<i style="background:rgba(0, 0, 255, 0.7);width:20px;height:20px;float:left;"></i>&emsp;Low<br>
&emsp;<i style="background:rgba(0, 255, 0, 0.7);width:20px;height:20px;float:left;"></i>&emsp;Medium<br>
&emsp;<i style="background:rgba(255, 255, 0, 0.7);width:20px;height:20px;float:left;"></i>&emsp;High<br>
&emsp;<i style="background:rgba(255, 0, 0, 0.7);width:20px;height:20px;float:left;"></i>&emsp;Very High<br>
&emsp;<i style="background:rgba(255, 0, 0, 1.0);width:20px;height:20px;float:left;"></i>&emsp;Tsunami<br>
</div>
'''

# Add the legend to the map
m.get_root().html.add_child(folium.Element(legend_html))

# Save the map to an HTML file
heatmap_path = 'heatmap.html'
m.save(heatmap_path)

# Display the heatmap within the notebook
display(IFrame(src=heatmap_path, width=1000, height=700))


# 

# In[ ]:





# In[ ]:
