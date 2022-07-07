
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
query="SELECT   NAME, COUNTRY,PRCP,TAVG, date_dim.Year, lieu_Dim.STATION ,lieu_Dim.LATITUDE ,lieu_Dim.LONGITUDE " \
      " FROM weather_fact, date_dim, lieu_Dim" \
      " WHERE weather_fact.DATE_ID = date_dim.DATE_ID and weather_fact.STATION = lieu_Dim.STATION"


df=pd.read_sql(query,connection)
df['PRCP'] = df['PRCP'].astype(float)
df['TAVG'] = df['TAVG'].astype(float)

df=df.groupby(['COUNTRY','NAME','STATION','LONGITUDE','LATITUDE','Year'])[['PRCP','TAVG']].mean()
df.reset_index(inplace=True)


#df = pd.read_csv('sql.csv')
#df2 = pd.read_csv('sql2.csv')
fig = px.scatter_geo(
    data_frame=df,
    locationmode='country names',
    lat=df["LATITUDE"],
    lon=df["LONGITUDE"],
    scope='africa',

    color_continuous_scale=px.colors.sequential.Viridis_r,
    template='plotly_white',
    height=1080

)

app.layout = html.Div([

    html.H1("Weather Dashboard", style={"text-align": 'center'}),

    dcc.Dropdown(id='slct_year',
                 options=[{'label': c, 'value': c}
                          for c in (df['Year'].sort_values(ascending=False).unique())],
                     multi=True,
                     placeholder="select a year",
                     # value=df['Year'].unique(),
                     style={'width': "40%"}
                     ),
# dcc.Checklist( id='slct_Country_check_list'
#     [
#         {
#             "label": html.Div(
#                 [
#                     html.Img(src="/assets/images/algerie.png"),
#                     html.Div("Python", style={'font-size': 15, 'padding-left': 10}),
#                 ], style={'display': 'flex', 'align-items': 'center', 'justify-content': 'center'}
#             ),
#             "value": "Python",
#         },
#         {
#             "label": html.Div(
#                 [
#                     html.Img(src="/assets/images/maroc.png"),
#                     html.Div("Julia", style={'font-size': 15, 'padding-left': 10}),
#                 ], style={'display': 'flex', 'align-items': 'center', 'justify-content': 'center'}
#             ),
#             "value": "Julia",
#         },
#         {
#             "label": html.Div(
#                 [
#                     html.Img(src="/assets/images/tunisie.png"),
#                     html.Div("R", style={'font-size': 15, 'padding-left': 10}),
#                 ], style={'display': 'flex', 'align-items': 'center', 'justify-content': 'center'}
#             ),
#             "value": "R",
#         },
#     ]
# ),
    dcc.Dropdown(id='slct_Country',
                 options=[{'label': c, 'value': c}
                          for c in (df['COUNTRY'].unique())],
                     multi=True,
                     placeholder="select a Country",
                     value=['DZA'],
                     style={'width': "40%"}
                     ),
    dcc.Dropdown(id='slct_filter',
                options=[
                    {"label": "PRCP", "value": "PRCP"},
                    {"label": "TAVG", "value": "TAVG"},],
                     multi=True,
                     placeholder="select a year",
                     value=["PRCP"],
                     style={'width': "40%"}
                     ),
    dcc.Graph(id='sales_map', figure=fig)
])


@app.callback(
    Output(component_id='sales_map', component_property='figure'),
    [Input(component_id='slct_year', component_property='value'),
     Input(component_id='slct_Country', component_property='value'),
     Input(component_id='slct_filter', component_property='value'),
]
    )



def update_graph(option_slctd,option_slctd1,option_slctd2):
    # print(option_slctd)
    # print(option_slctd1)
    # print(option_slctd2)
    dff=df.copy()
    # print(dff[dff.columns[dff.columns.isin(['PRCP'])]])


    if not option_slctd ==[]:
        if not isinstance(option_slctd,list):
            option_slctd=[option_slctd]

    dff=dff[dff["Year"].isin(option_slctd)&dff["COUNTRY"].isin(option_slctd1)]

    fig = px.scatter_geo(
            data_frame=dff,
            lat=dff["LATITUDE"],
            lon=dff["LONGITUDE"],
            scope='africa',
            hover_data=['PRCP','TAVG','NAME'],
            #color=  dff.columns[dff.columns.isin(['PRCP'])],


            color_continuous_scale=px.colors.sequential.Viridis_r,
            template='plotly_white'
    )
    fig.update_geos(
                     oceancolor='#b6e3fd',
                    fitbounds='geojson',
                    center=dict(lat=33.8, lon=2.9),
                     projection_scale=2.3,
                    showocean=True,
                    showcoastlines=True)



    return fig


if __name__ == '__main__':
    app.run_server(debug=True)