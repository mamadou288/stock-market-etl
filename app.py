import streamlit as st  
import pandas as pd
import plotly.graph_objects as go
import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

def get_stock_data(symbol, days=7):
    """Fetch stock data from database"""
    conn = None
    try:
        conn = psycopg2.connect(
            **DB_CONFIG,
            options='-c client_encoding=UTF8'
        )
        query = """
            SELECT 
                timestamp::timestamp, 
                open::float, 
                high::float, 
                low::float, 
                close::float, 
                volume::integer
            FROM stock_data 
            WHERE symbol = %s 
            AND timestamp >= NOW() - INTERVAL %s DAY
            ORDER BY timestamp
        """
        
        df = pd.read_sql_query(query, conn, params=(symbol, days))
        return df
    except Exception as e:
        print(f"Database error: {str(e)}")
        raise e
    finally:
        if conn:
            conn.close()

def create_candlestick_chart(df, symbol):
    """Create a candlestick chart using plotly"""
    fig = go.Figure(data=[go.Candlestick(x=df['timestamp'],
                open=df['open'],
                high=df['high'],
                low=df['low'],
                close=df['close'])])
    
    fig.update_layout(
        title=f'{symbol} Stock Price',
        yaxis_title='Price (USD)',
        xaxis_title='Date'
    )
    
    return fig

def main():
    st.set_page_config(page_title="Stock Market Dashboard", layout="wide")
    
    st.title("Stock Market Dashboard")
    
    # Sidebar
    st.sidebar.header("Settings")
    symbol = st.sidebar.selectbox(
        "Select Stock Symbol",
        ["AAPL", "MSFT", "GOOGL", "AMZN", "META"]
    )
    
    days = st.sidebar.slider(
        "Days of History",
        min_value=1,
        max_value=30,
        value=7
    )
    
    # Main content
    try:
        df = get_stock_data(symbol, days)
        
        if not df.empty:
            # Display current price and changes
            latest = df.iloc[-1]
            previous = df.iloc[-2]
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric(
                    "Current Price",
                    f"${latest['close']:.2f}",
                    f"{((latest['close'] - previous['close'])/previous['close']*100):.2f}%"
                )
            
            with col2:
                st.metric(
                    "Daily High",
                    f"${latest['high']:.2f}"
                )
            
            with col3:
                st.metric(
                    "Daily Low",
                    f"${latest['low']:.2f}"
                )
            
            # Display candlestick chart
            st.plotly_chart(create_candlestick_chart(df, symbol), use_container_width=True)
            
            # Display raw data
            if st.checkbox("Show Raw Data"):
                st.dataframe(df)
        else:
            st.warning("No data available for the selected period")
            
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")

if __name__ == "__main__":
    main() 