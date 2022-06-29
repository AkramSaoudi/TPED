
import pymysql
import pandas as pd
from dash import *
import plotly.express as px
app= Dash(__name__)
connection = pymysql.connect(host='localhost',
                             user='root',
                             password='1234',
                             database='Weatherdw',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
cursor = connection.cursor()

#import data from the data warehouse

query="SELECT   PRCP, date_dim.Year, lieu_Dim.STATION ,lieu_Dim.LATITUDE ,lieu_Dim.LONGITUDE " \
      " FROM weather_fact, date_dim, lieu_Dim" \
      " WHERE weather_fact.DATE_ID = date_dim.DATE_ID and weather_fact.STATION = lieu_Dim.STATION"
df3=pd.read_sql(query,connection)
df3['PRCP'] = df3['PRCP'].astype(float)
df3=df3.groupby(['STATION','LONGITUDE','LATITUDE','Year'])[['PRCP']].mean()
df3.reset_index(inplace=True)
print(df3[:15])

#---------------------------
#App Layout

app.layout= html.Div([

    html.H1("Sales Dashboard",style={"text-align":'center'}),

    dcc.Dropdown(id='slct_year',
                 options=[
                     {"label":"All","value":"All"},
                     {"label":"2010","value":"2010"},
                     {"label":"2011","value":"2011"},
                     {"label":"2012","value":"2012"},
                     {"label":"2013","value":"2013"},
                     {"label":"2014","value":"2014"},
                     {"label":"2015","value":"2015"},
                     {"label":"2016","value":"2016"},
                     {"label":"2017","value":"2017"},
                     {"label":"2018","value":"2018"}],
                 multi=True,
                 placeholder="select a year",
                 value=["All"],
                 style={'width':"40%"}
                 ),
    html.Div(id='output_container', children=[]),
    html.Br(),
    dcc.Graph(id='sales_map',figure={})
])



#-----------------------------------------
#connect the plotly graphs with Dash Components

@app.callback(
    [Output(component_id='output_container',component_property='children'),
     Output(component_id='sales_map',component_property='figure'),
     Input(component_id='slct_year',component_property='value')])


def update_graph(option_slctd):
    print(option_slctd)
    print(type(option_slctd))

    container="The year chosen by user was: {}".format(option_slctd)
    dff=df3.copy()
    if not option_slctd ==["All"]:
        if not isinstance(option_slctd,list):
            option_slctd=[option_slctd]
        dff=dff[df3["Year"].isin(option_slctd)]
    #plotly Express
    fig=px.choropleth(
        data_frame=dff,
        locationmode='country names',
        locations='Country',
        scope='world',
        color='Totalprofit',
        hover_data=['Country','Totalprofit'],
        color_continuous_scale=px.colors.sequential.Viridis_r,
        labels={'Total profit'},
        template='plotly_white')
    fig.update_layout(
        title_text='Total profit by country',
        geo_scope='world',
        plot_bgcolor="#323150",
        paper_bgcolor='#323130',
        margin=go.layout.Margin(l=0,r=0,t=35,b=0),
        font=dict(color='white')
        )
    return container, fig



#----------
if __name__ =='__main__':
    app.run_server(debug=True)
