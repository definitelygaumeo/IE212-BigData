# CVD_Prediction_System/visualization_app/app.py
import os
import sys
import dash
from dash import dcc, html, Input, Output
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import pymongo

# Thêm thư mục cha vào sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.dirname(current_dir)
sys.path.insert(0, project_dir)

# Initialize the Dash app
app = dash.Dash(__name__, title="CVD Prediction Dashboard")

# Connect to MongoDB
def get_data_from_mongodb(collection_name="predictions", limit=1000):
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["cvd_db"]
    collection = db[collection_name]
    
    # Get the data
    cursor = collection.find().limit(limit)
    data = list(cursor)
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    return df

# Fallback to CSV if MongoDB is not available
def get_data_from_csv():
    data_path = os.path.join(project_dir, "data_result", "predictions.csv")
    if os.path.exists(data_path):
        return pd.read_csv(data_path)
    
    # Fallback to raw data if predictions not available
    data_path = os.path.join(project_dir, "data_raw", "CVD_cleaned.csv")
    return pd.read_csv(data_path)

# Try to get data from MongoDB, fallback to CSV
try:
    df = get_data_from_mongodb()
    data_source = "MongoDB"
except:
    df = get_data_from_csv()
    data_source = "CSV"

# Layout
app.layout = html.Div([
    html.H1("CVD Prediction Dashboard", style={"textAlign": "center"}),
    html.H4(f"Data source: {data_source}", style={"textAlign": "center"}),
    
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
            )
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
                ])
            ])
        ], style={"width": "75%", "padding": "20px"})
    ], style={"display": "flex", "flexDirection": "row"})
])

# Callbacks
@app.callback(
    [Output("heart-disease-distribution", "figure"),
     Output("bmi-age-analysis", "figure"),
     Output("risk-factors", "figure")],
    [Input("age-filter", "value"),
     Input("sex-filter", "value"),
     Input("health-filter", "value")]
)
def update_graphs(selected_ages, selected_sexes, selected_health):
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
    
    return fig1, fig2, fig3

# Run the app
if __name__ == "__main__":
    app.run_server(debug=True)