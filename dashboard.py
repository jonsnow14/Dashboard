import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Output, Input
import plotly.graph_objs as go
import plotly.express as px
import psycopg2
from datetime import datetime, timedelta

# Connect to PostgreSQL database
conn = psycopg2.connect(
    host="your_host",
    database="your_database",
    user="your_username",
    password="your_password"
)
cursor = conn.cursor()

# Create Dash application
app = dash.Dash(__name__)

# Dashboard layout
app.layout = html.Div([
    html.H1("Data Ingestion Dashboard"),
    html.Div(id='live-update-text'),
    dcc.Graph(id='daily-graph'),
    dcc.Graph(id='hourly-graph'),
    dcc.Graph(id='monthly-graph'),
    dcc.Graph(id='traffic-graph'),
    dcc.Interval(
        id='interval-component',
        interval=5000,  # Update interval in milliseconds
        n_intervals=0
    )
])


# Update text and graphs
@app.callback(
    Output('live-update-text', 'children'),
    Output('daily-graph', 'figure'),
    Output('hourly-graph', 'figure'),
    Output('monthly-graph', 'figure'),
    Output('traffic-graph', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_data(n):
    # Calculate time ranges
    now = datetime.now()
    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_of_hour = now.replace(minute=0, second=0, microsecond=0)
    start_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    # Fetch data ingestion information from the database
    cursor.execute("SELECT timestamp, table_name, rows_inserted, last_update_statement, last_update_id FROM data_ingestion")
    records = cursor.fetchall()

    # Prepare data for daily graph
    daily_data = [0] * 24
    for record in records:
        timestamp, rows_inserted = record[0], record[2]
        if start_of_day <= timestamp <= now:
            hour = timestamp.hour
            daily_data[hour] += rows_inserted

    # Prepare data for hourly graph
    hourly_data = []
    for record in records:
        timestamp, rows_inserted = record[0], record[2]
        if start_of_hour <= timestamp <= now:
            hourly_data.append((timestamp, rows_inserted))
    hourly_timestamps, hourly_rows_inserted = zip(*hourly_data)

    # Prepare data for monthly graph
    monthly_data = []
    for record in records:
        timestamp, rows_inserted = record[0], record[2]
        if start_of_month <= timestamp <= now:
            monthly_data.append((timestamp, rows_inserted))
    monthly_timestamps, monthly_rows_inserted = zip(*monthly_data)

    # Prepare data for traffic graph
    table_traffic = {}
    last_updates = {}
    for record in records:
        table_name, rows_inserted, last_update_statement, last_update_id = record[1], record[2], record[3], record[4]
        if table_name not in table_traffic:
            table_traffic[table_name] = 0
            last_updates[table_name] = (last_update_statement, last_update_id)
        table_traffic[table_name] += rows_inserted

    sorted_traffic = sorted(table_traffic.items(), key=lambda x: x[1], reverse=True)
    table_names, traffic_values = zip(*sorted_traffic)

    # Create daily graph
    daily_trace = go.Scatter(
        x=list(range(24)),
        y=daily_data,
        mode='lines',
        name='Daily'
    )
    daily_layout = go.Layout(
        title='Data Ingestion - Daily',
        xaxis=dict(title='Hour'),
        yaxis=dict(title='Rows Inserted')
    )
    daily_figure = go.Figure(data=[daily_trace], layout=daily_layout)

    # Create hourly graph
    hourly_trace = go.Scatter(
        x=hourly_timestamps,
        y=hourly_rows_inserted,
        mode='lines',
        name='Hourly'
    )
    hourly_layout = go.Layout(
        title='Data Ingestion - Hourly',
        xaxis=dict(title='Timestamp'),
        yaxis=dict(title='Rows Inserted')
    )
    hourly_figure = go.Figure(data=[hourly_trace], layout=hourly_layout)

    # Create monthly graph
    monthly_trace = go.Scatter(
        x=monthly_timestamps,
        y=monthly_rows_inserted,
        mode='lines',
        name='Monthly'
    )
    monthly_layout = go.Layout(
        title='Data Ingestion - Monthly',
        xaxis=dict(title='Timestamp'),
        yaxis=dict(title='Rows Inserted')
    )
    monthly_figure = go.Figure(data=[monthly_trace], layout=monthly_layout)

    # Create traffic graph
    traffic_trace = go.Bar(
        x=list(table_names),
        y=list(traffic_values),
        name='Traffic'
    )
    traffic_layout = go.Layout(
        title='Table Traffic',
        xaxis=dict(title='Table Name'),
        yaxis=dict(title='Rows Inserted')
    )
    traffic_figure = go.Figure(data=[traffic_trace], layout=traffic_layout)

    # Create race bar chart for traffic
    race_data = sorted_traffic[:10]  # Display top 10 tables
    race_table_names, race_traffic_values = zip(*race_data)
    race_figure = px.bar(
        race_data,
        x=list(race_traffic_values),
        y=list(race_table_names),
        orientation='h',
        labels={'x': 'Rows Inserted', 'y': 'Table Name'},
        title='Table Traffic - Top 10'
    )

    # Update the text and return the figures
    return f"Last updated: {n} times", daily_figure, hourly_figure, monthly_figure, race_figure


if __name__ == '__main__':
    app.run_server(debug=True)
