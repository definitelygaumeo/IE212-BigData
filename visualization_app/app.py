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

def has_col(col):
    return col in df.columns

# -------------------- 1) XÁC ĐỊNH CÁC CỘT PHÂN LOẠI --------------------
CATEGORICAL_COLS = [
    "Age_Category", "Sex", "General_Health",          # đang dùng
    "Smoking_History", "Exercise",
    "Skin_Cancer", "Other_Cancer", "Depression",
    "Diabetes", "Arthritis"
]
cat_cols_present = [c for c in CATEGORICAL_COLS if c in df.columns]

# -------------------- 2) TẠO DROPDOWN ĐỘNG ------------------------------
dropdown_components = []
for col in cat_cols_present:
    dropdown_components.extend([
        html.Label(col.replace("_", " ")),
        dcc.Dropdown(
            id=f"{col}-filter",
            options=[{"label": v, "value": v} for v in df[col].unique()],
            value=[], multi=True
        )
    ])

# -------------------- 3) THAY phần Filters trong layout ------------------
filters_card = html.Div(
    [html.H3("Filters")] + dropdown_components +
    [html.Div([
        html.H4("Live Streaming Data", style={"marginTop": "20px"}),
        html.P(id="streaming-info", children="Waiting for data...")
    ])],
    style={"width": "25%", "padding": "20px",
           "backgroundColor": "#f8f9fa", "borderRadius": "10px"}
)

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
        filters_card,
        
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

# -------------------- 4) TẠO DANH SÁCH Input ĐỘNG ------------------------
dynamic_inputs = [Input(f"{c}-filter", "value") for c in cat_cols_present]

# Callback chính
@app.callback(
    [Output("heart-disease-distribution", "figure"),
     Output("bmi-age-analysis", "figure"),
     Output("risk-factors", "figure"),
     Output("live-stream-graph", "figure")],
    dynamic_inputs + [Input("interval-component", "n_intervals")]
)
def update_graphs(*args):
    # cuối cùng trong *args là n_intervals
    n_intervals = args[-1]
    filter_values = args[:-1]                # các giá trị dropdown

    # 1. Nhận thêm record Kafka rồi dán vào df (giống trước) ----------------
    new_records = get_kafka_data()
    global df
    if new_records:
        try:
            df = pd.concat([df, pd.DataFrame(new_records)], ignore_index=True)
        except Exception as e:
            print("Kafka error:", e)

    # 2. Lọc theo tất cả dropdown hiện có -----------------------------------
    filtered = df.copy()
    for col, selected in zip(cat_cols_present, filter_values):
        if selected:
            filtered = filtered[filtered[col].isin(selected)]

    # ---- Các biểu đồ (giữ nguyên logic cũ, chỉ đổi biến nguồn) -----------
    heart_counts = filtered["Heart_Disease"].value_counts().reset_index()
    heart_counts.columns = ["Heart_Disease", "Count"]
    fig1 = px.pie(heart_counts, names="Heart_Disease", values="Count",
                  title="Heart Disease Distribution")

    fig2 = px.box(filtered, x="Age_Category", y="BMI", color="Heart_Disease",
                  title="BMI by Age Category and Heart Disease")

    # ---- xác định cột số hiện diện ---------
    numeric_cols = [
        ("Smoking_History", lambda x: (x == "Yes").mean()*100),
        ("Alcohol_Consumption", "mean"),
        ("Exercise", lambda x: (x == "Yes").mean()*100),
        ("Fruit_Consumption", "mean"),
        ("Green_Vegetables_Consumption", "mean"),
        ("FriedPotato_Consumption", "mean")
    ]

    agg_dict = {col: func for col, func in numeric_cols if col in filtered.columns}

    risk_factors = filtered.groupby("Heart_Disease").agg(agg_dict).reset_index()

    fig3 = go.Figure()
    for col, color in [
        ("Smoking_History",  "indianred"),
        ("Alcohol_Consumption","lightsalmon"),
        ("Exercise","lightgreen"),
        ("Fruit_Consumption","royalblue"),
        ("Green_Vegetables_Consumption","darkgreen"),
        ("FriedPotato_Consumption","gold")
    ]:
        if col in risk_factors:
            fig3.add_trace(go.Bar(
                x=risk_factors["Heart_Disease"], y=risk_factors[col],
                name=col.replace("_", " "), marker_color=color))

    fig3.update_layout(barmode="group",
                       title="Risk Factors by Heart Disease Status")

    latest = filtered.tail(50)
    fig4 = px.scatter(latest, x="BMI", y="Alcohol_Consumption",
                      color="Heart_Disease",
                      hover_data=["General_Health","Sex"],
                      title="Live Data Stream (Last 50 Records)")

    return fig1, fig2, fig3, fig4

# Run the app
if __name__ == "__main__":
    try:
        app.run(debug=True)  # Phương thức mới (Dash >= 2.0) 
    except AttributeError:
        app.run_server(debug=True)  # Phương thức cũ (Dash < 2.0)