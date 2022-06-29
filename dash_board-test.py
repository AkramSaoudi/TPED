# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

from dash import Dash

import plotly.graph_objects as go
import pymysql
import dash_core_components as dcc
import dash_html_components as html

import plotly.express as px
import pandas as pd
from dash.dependencies import Output, Input

app = Dash(__name__)

connection = pymysql.connect(host='localhost',
                             user='root',
                             password='1234',
                             database='Weatherdw',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

cursor = connection.cursor()
query="SELECT   NAME, COUNTRY,PRCP, date_dim.Year, lieu_Dim.STATION ,lieu_Dim.LATITUDE ,lieu_Dim.LONGITUDE " \
      " FROM weather_fact, date_dim, lieu_Dim" \
      " WHERE weather_fact.DATE_ID = date_dim.DATE_ID and weather_fact.STATION = lieu_Dim.STATION"


df3=pd.read_sql(query,connection)
df3['PRCP'] = df3['PRCP'].astype(float)
df3=df3.groupby(['NAME','STATION','LONGITUDE','LATITUDE','Year'])[['PRCP']].mean()
df3.reset_index(inplace=True)




gig = px.scatter_geo(df3, lat=df3["LATITUDE"], lon=df3["LONGITUDE"], color=df3["PRCP"], hover_name=df3["NAME"],scope="africa")
app.layout = html.Div(children=[
                 html.H1(children='Dash board climatique', style={'textAlign': 'center', 'color': '#7FDBFF'}),
                 html.Script(children='Dash board climatique'),

    dcc.Dropdown(id='slct_year',
                 options=[
                     {"label": "All", "value": "All"},
                     {"label": "2010", "value": "2010"},
                     {"label": "2011", "value": "2011"},
                     {"label": "2012", "value": "2012"},
                     {"label": "2013", "value": "2013"},
                     {"label": "2014", "value": "2014"},
                     {"label": "2015", "value": "2015"},
                     {"label": "2016", "value": "2016"},
                     {"label": "2017", "value": "2017"},
                     {"label": "2018", "value": "2018"}],
                 multi=True,
                 placeholder="select a year",
                 value=["All"],
                 style={'width': "40%"}
                 ),
    html.Div(id='output_container', children=[]),
    html.Div(style={'width': '100%', 'display': 'flex', 'flex-direction': 'column', 'padding': '0px 10px'},
                          children=[
                              dcc.Graph(style={'width': '1080px'},id='weather_map', figure=map)
                          ]
                          ),
             ])

@app.callback([
    Output(component_id='output_container',component_property='children'),
    Output(component_id='weather_map', component_property='figure'),
    Input(component_id='slct_year',component_property='value')
    ])
def update_graph(option_slctd):
    container="The year chosen by user was: {}".format(option_slctd)
    print(option_slctd)
    print(container)
    dff=df3.copy()
    if not option_slctd ==["All"]:
        if not isinstance(option_slctd,list):
            option_slctd=[option_slctd]
        dff=dff[df["Year"].isin(option_slctd)]
    map = px.scatter_geo( data_frame=dff, lat=dff["LATITUDE"], lon=dff["LONGITUDE"], color=dff["PRCP"], hover_name=dff["NAME"],scope="africa")
    map.update_layout(
        title_text='PRCP by country',)
    return map,container



if __name__ == '__main__':
    app.run_server(debug=True)