import os

# from IA import create_the_AI_training_dataset, train_model, use_model
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, FloatType
from tqdm import tqdm
import matplotlib.dates as mdates
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import time
from datetime import datetime
import dash
from dash import dcc, html
from dash.dependencies import Input, Output

app = dash.Dash(__name__)

# Define the layout of the app
app.layout = html.Div([
    dcc.Interval(
            id='interval-component',
            interval=5*1000,  # in milliseconds
            n_intervals=0
        ),
    dcc.Tabs(id="tabs-example", value='tab-1', children=[
        dcc.Tab(label='Values by Date for Each Sport', value='tab-1'),
        dcc.Tab(label='Heart Rate Data Visualization', value='tab-2'),
        dcc.Tab(label='Values by Hours', value='tab-3'),
        dcc.Tab(label='Heart Rate Data Visualization (Hourly)', value='tab-4'),
    ]),
    html.Div(id='tabs-content-example')
])

@app.callback(Output('tabs-content-example', 'children'),
              Input('tabs-example', 'value'),
              Input('interval-component', 'n_intervals'))
def affichage(tab, n_intervals):
    if tab == 'tab-1':
        return Values_by_Date_for_Each_Sport()
    elif tab == 'tab-2':
        return Heart_Rate_Data_Visualization()
    elif tab == 'tab-3':
        return Values_by_Hours()
    elif tab == 'tab-4':
        return Heart_Rate_Data_Visualization_Hourly()

def Values_by_Date_for_Each_Sport():
    name_file = (file_csv(
        "C:/Users/tomcareghi/Documents/ESGI/4IABD/S2/spark/test/test/src/main/Ressources/all_files_spark/group_by_days_sport.csv/"))
    df = pd.read_csv(
        'C:/Users/tomcareghi/Documents/ESGI/4IABD/S2/spark/test/test/src/main/Ressources/all_files_spark/group_by_days_sport.csv/' + name_file)
    df = df.sort_values(by=['date'])
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")

    # Create subplot for each sport
    sports = df['sport'].unique()

    subplot_count = 0
    titles = []
    for sport in sports:
        df_sport = df[df['sport'] == sport]

        if len(df_sport) >= 2:  # Only plot sports with at least 2 rows
            titles.append(sport)
            subplot_count += 1

    fig = make_subplots(rows=subplot_count, cols=1, subplot_titles=titles, vertical_spacing=0.05)

    subplot_count = 1
    for title in titles:
        df_sport = df[df['sport'] == title]

        fig.add_trace(
            go.Scatter(x=df_sport["date"], y=df_sport["avg_value"], mode='lines+markers', name='Average Value',
                       line=dict(color='blue')), row=subplot_count, col=1)
        fig.add_trace(go.Scatter(x=df_sport["date"], y=df_sport["min_value"], mode='lines+markers', name='Min Value',
                                 line=dict(color='green')), row=subplot_count, col=1)
        fig.add_trace(go.Scatter(x=df_sport["date"], y=df_sport["max_value"], mode='lines+markers', name='Max Value',
                                 line=dict(color='red')), row=subplot_count, col=1)
        subplot_count += 1

    fig.update_layout(height=200 * subplot_count, title_text="Values by Date for Each Sport", showlegend=True)
    fig.update_yaxes(range=[20, 200])

    for i in fig['layout']['annotations']:
        i['font'] = dict(size=12)

    # Update xaxis for each subplot
    for i in range(subplot_count - 1):
        fig.update_xaxes(range=[df['date'].min(), df['date'].max()], row=i + 1)

    return dcc.Graph(figure=fig)

def Values_by_Hours():
    name_file = (file_csv("C:/Users/tomcareghi/Documents/ESGI/4IABD/S2/spark/test/test/src/main/Ressources/all_files_spark/group_by_days_hour_sport.csv/"))
    df = pd.read_csv(
        'C:/Users/tomcareghi/Documents/ESGI/4IABD/S2/spark/test/test/src/main/Ressources/all_files_spark/group_by_days_hour_sport.csv/' + name_file)
    df["TS"] = pd.to_datetime(df["date"] + df["HOUR"].astype(str), format="%Y-%m-%d%H")
    df = df.sort_values(by=['TS'])

    # Plot data using plotly
    fig = px.line(df, x="TS", y="avg_value", color_discrete_sequence=['blue'], labels={'avg_value': 'Average Value'},
                  hover_data=["min_value", "max_value"])
    fig.add_scatter(x=df["TS"], y=df["min_value"], mode='lines', name='Min Value', line=dict(color='green'))
    fig.add_scatter(x=df["TS"], y=df["max_value"], mode='lines', name='Max Value', line=dict(color='red'))
    fig.update_layout(title='Values by Timestamp', xaxis_title='Timestamp', yaxis_title='Value',
                      yaxis=dict(range=[20, 200]))
    print("okok3")

    # fig.show()
    # time.sleep(5)
    return dcc.Graph(figure=fig)


def Heart_Rate_Data_Visualization():
    print("affichage 2")
    name_file = (file_csv("C:/Users/tomcareghi/Documents/ESGI/4IABD/S2/spark/test/test/src/main/Ressources/all_files_spark/group_by_days_sport.csv/"))
    df = pd.read_csv(
        'C:/Users/tomcareghi/Documents/ESGI/4IABD/S2/spark/test/test/src/main/Ressources/all_files_spark/group_by_days_sport.csv/' + name_file)
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
    df = df.sort_values(by=['date'])
    df["sport"] = df['sport'].apply(lambda x: 200 if x != "0" else 0)

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add the bar trace first
    fig.add_trace(go.Bar(x=df["date"], y=df["sport"], name='sport', marker_color='cyan', opacity=0.6))
    fig.add_trace(go.Scatter(x=df["date"], y=df["avg_value"], mode='lines', name='avg_value', line=dict(color='blue')),
                  secondary_y=False)
    fig.add_trace(go.Scatter(x=df["date"], y=df["min_value"], mode='lines', name='min_value', line=dict(color='green')),
                  secondary_y=False)
    fig.add_trace(go.Scatter(x=df["date"], y=df["max_value"], mode='lines', name='max_value', line=dict(color='red')),
                  secondary_y=False)

    fig.update_yaxes(range=[20, 200], secondary_y=False)

    fig.update_layout(
        title_text='Heart Rate Data Visualization',
        title_x=0.5,
        yaxis_range=[20, 200],
        xaxis=dict(
            title='Date',
            gridcolor='white',
            gridwidth=2,
        ),
        yaxis=dict(
            title='Heart Rate (BPM)',
            gridcolor='white',
            gridwidth=2,
        ),
        paper_bgcolor='rgb(243, 243, 243)',
        plot_bgcolor='rgb(243, 243, 243)',
    )

    return dcc.Graph(figure=fig)


def Heart_Rate_Data_Visualization_Hourly():
    name_file = (file_csv("C:/Users/tomcareghi/Documents/ESGI/4IABD/S2/spark/test/test/src/main/Ressources/all_files_spark/group_by_days_hour_sport.csv/"))

    df2 = pd.read_csv(
        'C:/Users/tomcareghi/Documents/ESGI/4IABD/S2/spark/test/test/src/main/Ressources/all_files_spark/group_by_days_hour_sport.csv/' + name_file)
    df2["TS"] = pd.to_datetime(df2["date"] + df2["HOUR"].astype(str), format="%Y-%m-%d%H")
    df2 = df2.sort_values(by=['TS'])
    df2["sport"] = df2['sport'].apply(lambda x: 200 if x != "0" else 0)

    fig2 = make_subplots(specs=[[{"secondary_y": True}]])
    # Add the bar trace first
    fig2.add_trace(go.Bar(x=df2["TS"], y=df2["sport"], name='sport', marker_color='cyan', opacity=0.6), secondary_y=True)
    fig2.add_trace(go.Scatter(x=df2["TS"], y=df2["avg_value"], mode='lines', name='avg_value', line=dict(color='blue')),
                   secondary_y=False)
    fig2.add_trace(
        go.Scatter(x=df2["TS"], y=df2["min_value"], mode='lines', name='min_value', line=dict(color='green')),
        secondary_y=False)
    fig2.add_trace(go.Scatter(x=df2["TS"], y=df2["max_value"], mode='lines', name='max_value', line=dict(color='red')),
                   secondary_y=False)

    fig2.update_yaxes(range=[20, 200], secondary_y=False)

    fig2.update_layout(
        title_text='Heart Rate Data Visualization (Hourly)',
        title_x=0.5,
        xaxis=dict(
            title='Timestamp',
            gridcolor='white',
            gridwidth=2,
        ),
        yaxis=dict(
            title='Heart Rate (BPM)',
            gridcolor='white',
            gridwidth=2,
        ),
        paper_bgcolor='rgb(243, 243, 243)',
        plot_bgcolor='rgb(243, 243, 243)',
    )

    return dcc.Graph(figure=fig2)


def file_csv(dossier):
    fichiers = os.listdir(dossier)
    for fichier in fichiers:
        if fichier.endswith('.csv'):
            return fichier


if __name__ == '__main__':
    app.run_server(debug=True)


