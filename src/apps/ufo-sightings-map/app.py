import os
from databricks import sql
import pandas as pd
import numpy as np
import dash
from dash import dcc, html, Input, Output, State, callback_context
import plotly.express as px
import dash_bootstrap_components as dbc
import dash_ag_grid as dag
from databricks.sdk.core import Config
from keplergl import KeplerGl
from dash.dependencies import Input, Output
import json
import h3
import requests
import threading
import datetime as dt
import time

import requests

mapState = {
    "bearing": 0,
    "dragRotate": False,
    "latitude": 38.7946,
    "longitude": -106.5348,
    "pitch": 0,
    "zoom": 3,
    "isSplit": False,
    "isViewportSynced": True,
    "isZoomLocked": False,
    "splitMapViewports": [],
}

visState = {
    "filters": [],
    "layers": [
        {
            "id": "h3_layer_1",
            "type": "hexagonId",
            "config": {
                "dataId": "all_counties",
                "label": "H3 Layer",
                "columns": {
                    "hex_id": "hex_id",
                },
                "isVisible": True,
                "visConfig": {
                    "opacity": 0.25,
                "colorRange": {
                    "name": "ColorBrewer YlOrRd",
                    "type": "sequential",
                    "category": "ColorBrewer",
                    "colors": [
                        "#ffffb2", "#fecc5c", "#fd8d3c", "#f03b20", "#bd0026"
                    ]
                }                },
                "colorField": {
                    "name": "sightings",
                    "type": "integer"
                },
                "colorScale": "quantile"

            }
        }
    ],
    "interactionConfig": {}
}


# Set up the app
app = dash.Dash(__name__)

# Ensure environment variable is set correctly
assert os.getenv(
    "DATABRICKS_WAREHOUSE_ID"
), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."

# List to hold streamed responses
response_list = []
stream_complete = True

def sqlQuery(query: str) -> pd.DataFrame:
    """Execute a SQL query and return the result as a pandas DataFrame."""
    cfg = Config()  # Pull environment variables for auth
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
        credentials_provider=lambda: cfg.authenticate
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            df = pd.DataFrame(rows, columns=columns)
        return df

def fmapi_stream(selected_county):
    global response_list
    global stream_complete

    print("SETTING STREAM COMPLETE TO FALSE")
    response_list = []
    stream_complete = False

    if selected_county:

        query = f"""SELECT datetime, comments, duration_seconds, city, state, shape
                            FROM mpelletier.summit.enriched_ufo_sightings
                            WHERE county = '{selected_county.replace("'","''")}'
                            """
        sightings = sqlQuery(query)

        # return f"The selected species is {selected_species}"
        databricks_host = os.getenv("DATABRICKS_HOST")
        databricks_token = os.getenv("SERVING_PAT")
        model = 'databricks-claude-3-7-sonnet'
        # model = 'databricks-dbrx-instruct'
        #endpoint = os.getenv("DATABRICKS_ENDPOINT")
        endpoint = f"https://{databricks_host}/serving-endpoints/{model}/invocations"

        # Define the headers, including the authorization token
        headers = {
            "Authorization": f"Bearer {databricks_token}",
            "Content-Type": "application/json"
        }

        # Define the payload for the request
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": "You are an expert on UFO. You will need to determine possible explanations based on the following fields: datetime, comments, duration_seconds, city, state, shape"},
                {"role": "user", "content": f"Can you explain what would be the best explanation for these ufo sightings: {sightings.to_csv(index=False)}"}
            ],
            "max_tokens": 256,
            "stream": True  # Enable streaming
        }

        print(endpoint, headers, payload)
        # Make the POST request with streaming enabled
        response = requests.post(endpoint, headers=headers, data=json.dumps(payload), stream=True)
        print(response)
        try:
            for chunk in response.iter_content(chunk_size=None):
                if chunk:
                    # print("CHUNK:", chunk)
                    new_chunks = chunk.decode('utf-8').replace('\n'," ").split('data:')
                    new_chunks = [x.strip() for x in new_chunks if len(x.strip())>0]
                    # print("NEW_CHUNKS", new_chunks)
                    for new_chunk in new_chunks:
                        # if new_chunk == "[DONE]":
                        #     break
                        # print("NEW_CHUNK", new_chunk)
                        new_chunk_data = json.loads(new_chunk)
                        # print("NEW_CHUNK_DATA:", new_chunk_data)
                        if new_chunk_data['choices'][0]['finish_reason'] == 'stop':
                            break
                        response_list.append(new_chunk_data['choices'][0]['delta']['content'])
            response_list.append(None)
        except Exception as e:
            response_list.append(None)
            print(f"An error occurred: {e}")

    print("SETTING STREAM COMPLETE TO TRUE")
    stream_complete = True

# Fetch the all counties data
def fetch_all_counties_data():
    stime = dt.datetime.now()
    try:
        data = sqlQuery("""SELECT h3_h3tostring(h3.cellid) as hex_id, county, sightings, id
                            FROM mpelletier.summit.enriched_counties
                        """)
        # Convert any ndarray columns to lists
        for col in data.columns:
            if isinstance(data[col].iloc[0], np.ndarray):
                data[col] = data[col].apply(list)
    except Exception as e:
        print(f"An error occurred in querying data: {str(e)}")
        data = pd.DataFrame()
    print(f"ALL COUNTIES QUERY TOOK:    {dt.datetime.now() - stime}")
    return data

# Fetch the specific counties data
def fetch_specific_counties_data(selected_county):
    try:
        print(selected_county) 
        stime = dt.datetime.now()
        query = f"""SELECT count(*) as ct
                            FROM mpelletier.summit.enriched_ufo_sightings
                            WHERE county = '{selected_county.replace("'","''")}'
                """
        count_df = sqlQuery(query)
        print(f"COUNT QUERY TOOK:    {dt.datetime.now() - stime}")
        count = (count_df['ct'].iloc[0])

        if count<10:
            resolution = 7
            zoom = 7
        elif count<100:
            resolution = 6
            zoom = 6
        elif count<1000:
            resolution = 6
            zoom = 5
        else:
            resolution = 5
            zoom = 3
        print(f"Count: {count}, Resolution: {resolution}")

        stime = dt.datetime.now()
        query = f"""SELECT h3_h3tostring(h3.cellid) as hex_id,geometry as polygon, county, sightings, id
                            FROM mpelletier.summit.enriched_counties
                            WHERE county = '{selected_county.replace("'","''")}'
                """
        h3data = sqlQuery(query)
        print(f"H3 QUERY TOOK:       {dt.datetime.now() - stime}")
        # Convert any ndarray columns to lists
        for col in h3data.columns:
            if isinstance(h3data[col].iloc[0], np.ndarray):
                h3data[col] = h3data[col].apply(list)

        stime = dt.datetime.now()
        query = f"""SELECT geometry as wkt, county, datetime, comments, duration_seconds, airport_closed_by, urban, latitude, longitude
                            FROM mpelletier.summit.enriched_ufo_sightings
                            WHERE county = '{selected_county.replace("'","''")}'
                            """
        polygondata = sqlQuery(query)
        print(f"POLYGON QUERY TOOK:  {dt.datetime.now() - stime}")
        # Convert any ndarray columns to lists
        for col in polygondata.columns:
            if isinstance(polygondata[col].iloc[0], np.ndarray):
                polygondata[col] = polygondata[col].apply(list)
    except Exception as e:
        print(f"An error occurred in querying data: {str(e)}")
        h3data = pd.DataFrame()
        polygondata = pd.DataFrame()
    return h3data, polygondata, zoom

def fetch_distinct_counties_data():
    try:
        data = sqlQuery("SELECT DISTINCT county FROM mpelletier.summit.enriched_ufo_sightings ORDER BY county")
        # Convert any ndarray columns to lists
        for col in data.columns:
            if isinstance(data[col].iloc[0], np.ndarray):
                data[col] = data[col].apply(list)
    except Exception as e:
        print(f"An error occurred in querying data: {str(e)}")
        data = pd.DataFrame()
    return data

# Function to create a Kepler.gl map and save as an HTML file
# def create_kepler_map(first_data, second_data, third_data, filepath="kepler_map.html"):
def create_kepler_map(first_data, filepath="kepler_map.html"):
    print("CREATE KEPLER MAP...")
    map_ = KeplerGl(height=600, use_arrow=True)

    map_.add_data(data=first_data, name="all_counties")

    config = {
        "mapState": mapState,
        "visState": visState
    }

    map_.config = config

    # Save the map to an HTML file
    map_.save_to_html(file_name=filepath)
    return filepath

def create_new_kepler_map(first_data, second_data, zoom, filepath="kepler_map.html"):
    stime = dt.datetime.now()
    #first_data[['latitude', 'longitude']] = first_data['hex_id'].apply(lambda x: pd.Series(h3.cell_to_latlng(x)))
    average_latitude = second_data['latitude'].mean()
    average_longitude = second_data['longitude'].mean()

    map_ = KeplerGl(height=600, use_arrow=True)
    map_.add_data(data=first_data, name="all_counties")
    map_.add_data(data=second_data, name="points")

    mapState['latitude'] = average_latitude
    mapState['longitude'] = average_longitude
    mapState['zoom'] = zoom

    visState['layers'][0] = {
                "id": "county-layer",
                "type": "geojson",
                "config": {
                    "dataId": "all_counties",
                    "label": "WKT polygon",
                    "columns": {"geojson": "polygon"},
                    "isVisible": True,
                    "visConfig": {
                        "opacity": 0.8,
                        "strokeColor": [234, 221, 208]
                    },
                },
            }
    visState['layers'] = [visState['layers'][0]]
    #print(visState)
    visState['layers'].append({
                    "id": "sightings-layer",
                    "type": "geojson",
                    "config": {
                        "dataId": "points",
                        "label": "WKT points",
                        "color": [30, 150, 190],
                        "columns": {"geojson": "wkt"},
                        "isVisible": True,
                        "visConfig": {
                            "opacity": 0.8,
                            "strokeColor": [0, 250, 25],
                            "thickness": 4,
                            "strokeColorOpacity": 0.8,
                            "fillColor": [30, 150, 190],
                            "radius": 1,
                            "strokeWidth": 4
                        },
                    },
                })

    config = {
        "mapState": mapState,
        "visState": visState
    }
    map_.config = config

    # Save the map to an HTML file
    map_.save_to_html(file_name=filepath)
    # print(f"TIME TO NEW MAP: {dt.datetime.now() - stime}")
    return filepath

all_counties_data = fetch_all_counties_data()
distinct_counties_data = fetch_distinct_counties_data()

# Generate the map and save to HTML
map_filepath = create_kepler_map(all_counties_data)

# Read the HTML content
with open(map_filepath, "r") as f:
    map_html = f.read()

app.layout = html.Div(
    [
        dcc.Dropdown(
            id='user-dropdown',
            options=[{'label': county, 'value': county} for county in distinct_counties_data['county'].unique()],
            placeholder='Select a county',
            style={
                "font-family": "Helvetica",
                'fontSize': '14px'
            }
        ),
        html.Iframe(
            id="map-iframe",
            srcDoc=map_html,
            style={"width": "100%", "height": "100vh", "border": "none"},
        ),
        html.Div(
            children="Select a county from the dropdown list above the map.",
            id="map-textbox",
            style={
                "font-family": "Helvetica",
                "position": "absolute",
                "top": "95%",
                "left": "50%",
                "transform": "translate(-50%, -50%)",
                "backgroundColor": "rgba(0, 0, 0, 0.5)",  # Semi-transparent black
                "color": "white",
                "padding": "20px",
                "borderRadius": "10px",
                "textAlign": "left",
                "text-wrap": "pretty",
                # "overflowY": "scroll"
            },
        ),
        dcc.Interval(id="interval-component", interval=100, n_intervals=0, disabled=True),  # Check every n milliseconds
    ],
    style={"backgroundColor": "#29323C"}
)

@app.callback(
    Output('map-iframe', 'srcDoc'), #,Output("interval-component", "disabled", allow_duplicate=True)],
    Input('user-dropdown', 'value'),
    prevent_initial_call=True,
)
def update_map(selected_county):
    if selected_county:
        new_all_county_data, new_county_polygon_data, zoom = fetch_specific_counties_data(selected_county)
        
        # Generate the map and save to HTML
        new_map_filepath = create_new_kepler_map(new_all_county_data, new_county_polygon_data, zoom)

        # Read the HTML content
        with open(new_map_filepath, "r") as f:
            new_map_html = f.read()

        return new_map_html #, False  # Update iframe with new map data
    
    return map_html #, True  # Return initial map if no user is selected

# # Callback to start iterative text update
@app.callback(
    Output("interval-component", "disabled", allow_duplicate=True),
    Input('map-iframe', 'srcDoc'),
    State("user-dropdown", "value"),
    prevent_initial_call=True
)
def start_text_update(srcDoc, selected_county):
    print("START TEXT UPDATE selected_county:", selected_county)
    if selected_county:
        threading.Thread(target=fmapi_stream, args=(selected_county,)).start()
        print("Turning on the interval-component")
        return False 
    else:
        return True

@app.callback(
    [Output("map-textbox", "children"), Output("interval-component", "disabled", allow_duplicate=True)],
    [Input("interval-component", "n_intervals")],
    prevent_initial_call=True,
)
def update_response(n_intervals):
    global response_list
    global stream_complete

    if not stream_complete:
        # print("STREAM IS IN PROCESS")
        return "".join([x for x in response_list if x is not None]), False
    else:
        print("STREAM IS DONE")
        if len(response_list)>0:
            final_results = [x for x in response_list if x is not None]
            print("RETURNING:", "".join(final_results))
            return "".join(final_results), True
        response_list = []
        return dash.no_update, True

if __name__ == "__main__":
    app.run_server(debug=True)