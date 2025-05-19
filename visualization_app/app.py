import os
import sys
import dash
from dash import dcc, html, Input, Output
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import json
from kafka import KafkaConsumer
import time

# Thêm thư mục cha vào sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.dirname(current_dir)
sys.path.insert(0, project_dir)

# Initialize the Dash app
app = dash.Dash(__name__, title="CVD Prediction Dashboard")

# Hàm lấy dữ liệu từ Kafka
def get_kafka_data(timeout=1000):
    data_list = []
    try:
        consumer = KafkaConsumer(
            'cvd_topic',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='latest',
            consumer_timeout_ms=timeout,  # Timeout sau 1 giây
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='dashboard-consumer'
        )
        
        start_time = time.time()
        while time.time() - start_time < timeout/1000:
            for message in consumer:
                data_list.append(message.value)
            time.sleep(0.1)
    except Exception as e:
        print(f"Error connecting to Kafka: {e}")
    
    return data_list

def get_data_from_csv():
    data_path = os.path.join(project_dir, "data_result", "predictions.csv")
    if os.path.exists(data_path):
        return pd.read_csv(data_path)
    
    # Fallback to raw data if predictions not available
    data_path = os.path.join(project_dir, "data_raw", "CVD_cleaned.csv")
    return pd.read_csv(data_path)

# Khởi tạo dữ liệu từ CSV
df = get_data_from_csv()
data_source = "CSV + Kafka (Real-time)"

# Layout
app.layout = html.Div([
    html.H1("CVD Prediction Dashboard", style={"textAlign": "center"}),
    html.H4(f"Data source: {data_source}", style={"textAlign": "center"}),
    
    # Thêm interval component để tự động cập nhật
    dcc.Interval(
        id='interval-component',
        interval=5*1000,  # Cập nhật mỗi 5 giây
        n_intervals=0
    ),
    
    html.Div([
        html.Div([
            html.H3("Filters"),
            html.Label("Age Category"),
            dcc.Dropdown(
                id="age-filter",
                options=[{"label": age, "value": age} for age in df["Age_Category"].unique()],
                value=[],
                multi=True
            ),
            html.Label("Sex"),
            dcc.Dropdown(
                id="sex-filter",
                options=[{"label": sex, "value": sex} for sex in df["Sex"].unique()],
                value=[],
                multi=True
            ),
            html.Label("Health Status"),
            dcc.Dropdown(
                id="health-filter",
                options=[{"label": health, "value": health} for health in df["General_Health"].unique()],
                value=[],
                multi=True
            ),
            
            # Thêm thông tin streaming
            html.Div([
                html.H4("Live Streaming Data", style={"marginTop": "20px"}),
                html.P(id="streaming-info", children="Waiting for data...")
            ])
        ], style={"width": "25%", "padding": "20px", "backgroundColor": "#f8f9fa", "borderRadius": "10px"}),
        
        html.Div([
            dcc.Tabs([
                dcc.Tab(label="Distribution", children=[
                    html.Div([
                        dcc.Graph(id="heart-disease-distribution")
                    ])
                ]),
                dcc.Tab(label="BMI Analysis", children=[
                    html.Div([
                        dcc.Graph(id="bmi-age-analysis")
                    ])
                ]),
                dcc.Tab(label="Risk Factors", children=[
                    html.Div([
                        dcc.Graph(id="risk-factors")
                    ])
                ]),
                # Thêm tab mới cho dữ liệu streaming
                dcc.Tab(label="Live Stream", children=[
                    html.Div([
                        dcc.Graph(id="live-stream-graph")
                    ])
                ])
            ])
        ], style={"width": "75%", "padding": "20px"})
    ], style={"display": "flex", "flexDirection": "row"})
])

# Callback để cập nhật thông tin streaming
@app.callback(
    Output("streaming-info", "children"),
    [Input("interval-component", "n_intervals")]
)
def update_streaming_info(n):
    return f"Last update: {time.strftime('%H:%M:%S')}"

# Callback chính
@app.callback(
    [Output("heart-disease-distribution", "figure"),
     Output("bmi-age-analysis", "figure"),
     Output("risk-factors", "figure"),
     Output("live-stream-graph", "figure")],
    [Input("age-filter", "value"),
     Input("sex-filter", "value"),
     Input("health-filter", "value"),
     Input("interval-component", "n_intervals")]  # Thêm interval trigger
)
def update_graphs(selected_ages, selected_sexes, selected_health, n_intervals):
    # Lấy dữ liệu từ Kafka và kết hợp với dữ liệu CSV
    new_data = get_kafka_data()
    
    # Kết hợp dữ liệu
    global df
    if new_data:
        try:
            new_df = pd.DataFrame(new_data)
            # Đảm bảo có các cột cần thiết
            if 'Age_Category' in new_df.columns and 'Sex' in new_df.columns and 'General_Health' in new_df.columns:
                df = pd.concat([df, new_df], ignore_index=True)
                print(f"Added {len(new_df)} new records from Kafka")
        except Exception as e:
            print(f"Error processing Kafka data: {e}")
    
    # Filter the data
    filtered_df = df.copy()
    
    if selected_ages:
        filtered_df = filtered_df[filtered_df["Age_Category"].isin(selected_ages)]
    
    if selected_sexes:
        filtered_df = filtered_df[filtered_df["Sex"].isin(selected_sexes)]
    
    if selected_health:
        filtered_df = filtered_df[filtered_df["General_Health"].isin(selected_health)]
    
    # Heart Disease Distribution
    heart_disease_counts = filtered_df["Heart_Disease"].value_counts().reset_index()
    heart_disease_counts.columns = ["Heart_Disease", "Count"]
    
    fig1 = px.pie(
        heart_disease_counts, 
        names="Heart_Disease", 
        values="Count",
        title="Heart Disease Distribution",
        color_discrete_sequence=px.colors.qualitative.Safe
    )
    
    # BMI by Age Category and Heart Disease
    fig2 = px.box(
        filtered_df,
        x="Age_Category",
        y="BMI",
        color="Heart_Disease",
        title="BMI by Age Category and Heart Disease",
        color_discrete_sequence=px.colors.qualitative.Safe
    )
    
    # Risk Factors (smoking, alcohol, exercise)
    risk_factors = filtered_df.groupby("Heart_Disease").agg({
        "Smoking_History": lambda x: (x == "Yes").mean() * 100,
        "Alcohol_Consumption": "mean",
        "Exercise": lambda x: (x == "Yes").mean() * 100
    }).reset_index()
    
    fig3 = go.Figure()
    
    fig3.add_trace(go.Bar(
        x=risk_factors["Heart_Disease"],
        y=risk_factors["Smoking_History"],
        name="Smoking History (%)",
        marker_color='indianred'
    ))
    
    fig3.add_trace(go.Bar(
        x=risk_factors["Heart_Disease"],
        y=risk_factors["Alcohol_Consumption"],
        name="Alcohol Consumption (avg)",
        marker_color='lightsalmon'
    ))
    
    fig3.add_trace(go.Bar(
        x=risk_factors["Heart_Disease"],
        y=risk_factors["Exercise"],
        name="Exercise (%)",
        marker_color='lightgreen'
    ))
    
    fig3.update_layout(
        barmode='group',
        title="Risk Factors by Heart Disease Status"
    )
    
    # Live Stream Graph - chỉ hiển thị 50 bản ghi mới nhất
    latest_records = filtered_df.tail(50)
    
    fig4 = px.scatter(
        latest_records,
        x="BMI", 
        y="Blood_Pressure_Systolic",
        color="Heart_Disease",
        size="Age",
        hover_data=["General_Health", "Sex"],
        title="Live Data Stream (Last 50 Records)",
        labels={"BMI": "Body Mass Index", "Blood_Pressure_Systolic": "Systolic BP"}
    )
    
    return fig1, fig2, fig3, fig4

# Run the app
if __name__ == "__main__":
    try:
        app.run(debug=True)  # Phương thức mới (Dash >= 2.0) 
    except AttributeError:
        app.run_server(debug=True)  # Phương thức cũ (Dash < 2.0)