# Dashboard shows:
#   1. Live price table  - all 50 stocks with price, change%, high, low, volume
#   2. Live price chart  - select any stock and see its price movement today
#   3. Top Gainer card   - stock with biggest % gain today
#   4. Top Loser card    - stock with biggest % loss today
#   5. Most Active card  - stock with highest volume today
#   6. Market Summary    - total advances vs declines count
#   7. End of Day chart  - bar chart of all 50 stocks sorted by % change
#                          This appears when market closes and shows full day performance
#
# Open your Brave browser and go to: http://localhost:8050
# Dashboard refreshes every 2 seconds automatically

import json
import time
import pytz #handles time zone
import threading #used to run multiple task simultaneously
from datetime import datetime
from kafka import KafkaConsumer
import mysql.connector
import dash #used to create web app
from dash import dcc #Dash core component(graphs, dropdowns, intervals)
from dash import html #html components like div, table, etc
from dash.dependencies import Input #used for dash callbacks
from dash.dependencies import Output
import plotly.graph_objects as go #used to create interactive charts

from config import (
    KAFKA_BROKER, KAFKA_TOPIC,
    DB_HOST, DB_USER, DB_PASSWORD, DB_NAME,
    MARKET_OPEN, MARKET_CLOSE
)

IST = pytz.timezone('Asia/Kolkata')

# This dictionary holds the latest tick data for every stock
# The Kafka background thread writes to this dictionary
# The dashboard reads from this dictionary every 2 seconds
# Structure: { 'RELIANCE': { 'ltp': 2847.5, 'change_percent': 1.2, ... } }
latest_prices = {}


def is_market_open():
    current_time = datetime.now(IST)
    current_total = current_time.hour * 60 + current_time.minute #convert current time into minutes
    open_total = MARKET_OPEN[0] * 60 + MARKET_OPEN[1] #convert market opening and closing times into minutes
    close_total = MARKET_CLOSE[0] * 60 + MARKET_CLOSE[1]
    if current_time.weekday() >= 5:
        return False
    if current_total >= open_total and current_total <= close_total: #Market is open
        return True
    else:
        return False


def get_new_db_connection():
    # Create a fresh MySQL connection
    # We call this when we need to query the database for chart data
    connection = mysql.connector.connect(
        host = DB_HOST,
        user = DB_USER,
        password = DB_PASSWORD,
        database = DB_NAME
    )
    return connection

def deserializer(v):
    return json.loads(v.decode('utf-8')) 

# Background thread: reads Kafka and updates latest_prices 
# This runs in background so dashboard can keep showing while data comes in
# Thread and dashboard both run at same time
def read_kafka_in_background():
    kafka_consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers = [KAFKA_BROKER],
        group_id = 'dashboard-consumer-group',
        # auto_offset_reset = 'latest' means only read NEW messages
        auto_offset_reset = 'latest',
        enable_auto_commit = True,
        isolation_level = 'read_committed', #Read only committed messages
        value_deserializer = deserializer,
        session_timeout_ms     = 30000, #Consumer considered dead after 30 seconds without heartbeat
        heartbeat_interval_ms  = 10000, #Send heartbeat every 10 seconds
        max_poll_interval_ms   = 300000 #Maximum processing time = 5 minutes
    )

    print("Background Kafka reader started...")

    for kafka_message in kafka_consumer:
        data = kafka_message.value
        symbol = data.get('symbol', '')

        if symbol == '':
            continue

        ltp = data.get('ltp', 0)
        open_price = data.get('open', 0)

        # Calculate change from open
        if open_price > 0:
            change = round(ltp - open_price, 2)
            change_percent = round(((ltp - open_price) / open_price) * 100, 2)
        else:
            change         = 0
            change_percent = 0

        # Store latest values for this stock
        # Dashboard reads this every 2 seconds
        latest_prices[symbol] = {
            'symbol': symbol,
            'ltp': ltp,
            'open': open_price,
            'high': data.get('high', 0),
            'low': data.get('low', 0),
            'volume': data.get('volume', 0),
            'change': change,
            'change_percent': change_percent,
            'exchange_time': data.get('exchange_time', 0)
        }


# Start the Kafka reader in a background thread
# daemon=True means this thread will stop automatically when main program stops
kafka_thread = threading.Thread(target=read_kafka_in_background, daemon=True)
kafka_thread.start()
print("Kafka background thread started")


# Build the Dash dashboard layout
# Dash is a Python library for building web dashboards
# html.Div is like a box on the page
# dcc.Graph shows a chart
# dcc.Interval refreshes the page every 2 seconds

app = dash.Dash(__name__)

app.layout = html.Div(children=[

    # Top bar with title and market status
    html.Div(children=[
        html.H1(
            children  = "Nifty 50 Live Market Dashboard",
            style     = {'color': 'white', 'textAlign': 'center', 'margin': '0', 'padding': '15px', 'fontSize': '24px'}
        ),
        html.P(
            id    = 'market-status-text',
            style = {'color': '#00ff88', 'textAlign': 'center', 'fontSize': '14px', 'margin': '0', 'paddingBottom': '10px'}
        )
    ], style={'backgroundColor': '#1a1a2e', 'borderBottom': '2px solid #00ff88'}),

    # Auto refresh component - triggers every 2000ms = 2 seconds
    dcc.Interval(id='auto-refresh', interval=2000, n_intervals=0),

    # Four summary cards in one row
    html.Div(children=[

        # Card 1: Top Gainer
        html.Div(
            id    = 'top-gainer-card',
            style = {
                'flex': '1', 'margin': '10px', 'padding': '15px',
                'backgroundColor': '#0d2137', 'borderRadius': '8px',
                'border': '1px solid #00ff88'
            }
        ),

        # Card 2: Top Loser
        html.Div(
            id    = 'top-loser-card',
            style = {
                'flex': '1', 'margin': '10px', 'padding': '15px',
                'backgroundColor': '#0d2137', 'borderRadius': '8px',
                'border': '1px solid #ff4444'
            }
        ),

        # Card 3: Most Active
        html.Div(
            id    = 'most-active-card',
            style = {
                'flex': '1', 'margin': '10px', 'padding': '15px',
                'backgroundColor': '#0d2137', 'borderRadius': '8px',
                'border': '1px solid #ffaa00'
            }
        ),

        # Card 4: Market Summary
        html.Div(
            id    = 'market-summary-card',
            style = {
                'flex': '1', 'margin': '10px', 'padding': '15px',
                'backgroundColor': '#0d2137', 'borderRadius': '8px',
                'border': '1px solid #8888ff'
            }
        )

    ], style={'display': 'flex', 'backgroundColor': '#0a0a1a', 'padding': '5px'}),

    # Stock dropdown and live price chart
    html.Div(children=[

        html.Label(
            children = "Select a stock to see its price chart:",
            style    = {'color': 'white', 'padding': '10px', 'display': 'block'}
        ),

        # Dropdown to pick which stock to chart
        dcc.Dropdown(
            id          = 'stock-dropdown',
            options     = [],
            value       = None,
            placeholder = 'Click here and select a stock...',
            style       = {'width': '300px', 'marginLeft': '10px', 'marginBottom': '10px'}
        ),

        # The live price chart for selected stock
        dcc.Graph(
            id    = 'live-price-chart',
            style = {'height': '350px'}
        )

    ], style={
        'backgroundColor': '#0a0a1a', 'margin': '10px',
        'borderRadius': '8px', 'border': '1px solid #333'
    }),

    # Live prices table
    html.Div(children=[
        html.H3(
            children = "Live Nifty 50 Prices",
            style    = {'color': 'white', 'padding': '10px', 'margin': '0'}
        ),
        html.Div(id='live-prices-table')
    ], style={
        'backgroundColor': '#0a0a1a', 'margin': '10px',
        'borderRadius': '8px', 'border': '1px solid #333'
    }),

    # End of Day Performance Section
    # Shows bar chart of all 50 stocks by % change
    # During market hours: shows intraday performance so far
    # After market closes: shows final day performance
    html.Div(children=[
        html.H3(
            id    = 'eod-title',
            style = {'color': 'white', 'padding': '10px', 'margin': '0'}
        ),
        dcc.Graph(
            id    = 'eod-performance-chart',
            style = {'height': '450px'}
        )
    ], style={
        'backgroundColor': '#0a0a1a', 'margin': '10px',
        'borderRadius': '8px', 'border': '1px solid #ffaa00'
    })

], style={'backgroundColor': '#0a0a1a', 'minHeight': '100vh', 'fontFamily': 'Arial, sans-serif'})


# Callback 1: Update market status text 
# A callback is a function that Dash calls automatically when something changes
# Input('auto-refresh', 'n_intervals') means: run this every time the timer fires
@app.callback(
    Output('market-status-text', 'children'),
    Input('auto-refresh', 'n_intervals')
)
def update_market_status(n):
    current_time = datetime.now(IST).strftime('%d %b %Y  %H:%M:%S IST')

    if is_market_open():
        return "● MARKET OPEN  |  " + current_time + "  |  Tracking " + str(len(latest_prices)) + " stocks live"
    else:
        return "● MARKET CLOSED  |  " + current_time + "  |  Showing last known prices"


#  Callback 2: Update stock dropdown options 
@app.callback(
    Output('stock-dropdown', 'options'),
    Input('auto-refresh', 'n_intervals')
)
def update_dropdown_options(n):
    options = []
    for symbol in sorted(latest_prices.keys()):
        options.append({'label': symbol, 'value': symbol})
    return options


#  Callback 3: Update all 4 summary cards 
@app.callback(
    Output('top-gainer-card','children'),
    Output('top-loser-card','children'),
    Output('most-active-card','children'),
    Output('market-summary-card','children'),
    Input('auto-refresh','n_intervals')
)
def update_summary_cards(n):
    # Show waiting message if no data yet
    waiting = html.P("Waiting for market data...", style={'color': '#888'})
    if len(latest_prices) == 0:
        return waiting, waiting, waiting, waiting

    all_stocks = list(latest_prices.values())

    # Sort by change percent to find biggest gainer and loser
    sorted_by_change = sorted(all_stocks, key=lambda x: x['change_percent'], reverse=True)
    sorted_by_volume = sorted(all_stocks, key=lambda x: x['volume'], reverse=True)

    # Separate gainers and losers
    gainers = []
    losers  = []
    for stock in all_stocks:
        if stock['change_percent'] > 0:
            gainers.append(stock)
        elif stock['change_percent'] < 0:
            losers.append(stock)

    #  Top Gainer card 
    if len(gainers) > 0:
        top_gainer    = sorted_by_change[0]
        gainer_symbol = top_gainer['symbol']
        gainer_ltp    = str(top_gainer['ltp'])
        gainer_change = "+" + str(top_gainer['change_percent']) + "%"

        gainer_card = html.Div(children=[
            html.P("🟢 TOP GAINER TODAY", style={'color': '#00ff88', 'fontWeight': 'bold', 'margin': '0', 'fontSize': '12px'}),
            html.H2(gainer_symbol, style={'color': 'white', 'margin': '5px 0', 'fontSize': '22px'}),
            html.P("Rs." + gainer_ltp, style={'color': 'white', 'margin': '0', 'fontSize': '18px'}),
            html.P(gainer_change, style={'color': '#00ff88', 'fontSize': '16px', 'fontWeight': 'bold', 'margin': '5px 0'})
        ])
    else:
        gainer_card = html.P("No gainers yet", style={'color': '#888'})

    #  Top Loser card 
    if len(losers) > 0:
        top_loser = sorted_by_change[-1]
        loser_symbol = top_loser['symbol']
        loser_ltp = str(top_loser['ltp'])
        loser_change = str(top_loser['change_percent']) + "%"

        loser_card = html.Div(children=[
            html.P("🔴 TOP LOSER TODAY", style={'color': '#ff4444', 'fontWeight': 'bold', 'margin': '0', 'fontSize': '12px'}),
            html.H2(loser_symbol, style={'color': 'white', 'margin': '5px 0', 'fontSize': '22px'}),
            html.P("Rs." + loser_ltp, style={'color': 'white', 'margin': '0', 'fontSize': '18px'}),
            html.P(loser_change, style={'color': '#ff4444', 'fontSize': '16px', 'fontWeight': 'bold', 'margin': '5px 0'})
        ])
    else:
        loser_card = html.P("No losers yet", style={'color': '#888'})

    #  Most Active card 
    most_active = sorted_by_volume[0]
    most_active_symbol = most_active['symbol']
    most_active_volume = str(most_active['volume'])
    most_active_ltp = str(most_active['ltp'])

    active_card = html.Div(children=[
        html.P("🟡 MOST ACTIVE TODAY", style={'color': '#ffaa00', 'fontWeight': 'bold', 'margin': '0', 'fontSize': '12px'}),
        html.H2(most_active_symbol, style={'color': 'white', 'margin': '5px 0', 'fontSize': '22px'}),
        html.P("Rs." + most_active_ltp, style={'color': 'white', 'margin': '0', 'fontSize': '18px'}),
        html.P("Volume: " + most_active_volume, style={'color': '#ffaa00', 'fontSize': '14px', 'margin': '5px 0'})
    ])

    #  Market Summary card 
    total_gainers = len(gainers)
    total_losers = len(losers)
    total_tracking = len(latest_prices)

    summary_card = html.Div(children=[
        html.P("📊 MARKET SUMMARY", style={'color': '#8888ff', 'fontWeight': 'bold', 'margin': '0', 'fontSize': '12px'}),
        html.P("Advances: " + str(total_gainers), style={'color': '#00ff88', 'fontSize': '16px', 'margin': '5px 0'}),
        html.P("Declines: " + str(total_losers), style={'color': '#ff4444', 'fontSize': '16px', 'margin': '5px 0'}),
        html.P("Tracking: " + str(total_tracking) + " stocks", style={'color': 'white', 'fontSize': '14px', 'margin': '5px 0'})
    ])

    return gainer_card, loser_card, active_card, summary_card


#  Callback 4: Update live price chart for selected stock 
@app.callback(
    Output('live-price-chart', 'figure'),
    Input('auto-refresh',    'n_intervals'),
    Input('stock-dropdown',  'value')
)
def update_price_chart(n, selected_stock):
    # Create empty dark chart if no stock selected
    empty_fig = go.Figure()
    empty_fig.update_layout(
        paper_bgcolor = '#0d2137',
        plot_bgcolor  = '#0a0a1a',
        font = {'color': 'white'},
        title = 'Select a stock from the dropdown above to see its chart'
    )

    if selected_stock is None:
        return empty_fig

    if selected_stock not in latest_prices:
        return empty_fig

    # Read price history from database for this stock
    # We read last 200 ticks to draw the line chart
    price_list = []
    time_list  = []

    try:
        db_conn = get_new_db_connection()
        db_cursor = db_conn.cursor()

        db_cursor.execute("""
            SELECT ltp, exchange_time
            FROM stock_ticks
            WHERE symbol = %s
            ORDER BY exchange_time DESC
            LIMIT 200
        """, (selected_stock,))

        rows = db_cursor.fetchall()
        db_conn.close()

        # Reverse so oldest tick is on the left side of chart
        rows.reverse()

        for row in rows:
            price_list.append(row[0])
            # Convert exchange_time (milliseconds) to readable time like "10:35:42"
            tick_time = datetime.fromtimestamp(row[1] / 1000, tz=IST)
            time_list.append(tick_time.strftime('%H:%M:%S'))

    except Exception as error:
        print("Chart data fetch error: " + str(error))
        # If DB fails, just show current price as single point
        price_list = [latest_prices[selected_stock]['ltp']]
        time_list = [datetime.now(IST).strftime('%H:%M:%S')]

    current_stock = latest_prices[selected_stock]

    # Use green line if stock is up today, red line if down
    if current_stock['change_percent'] >= 0:
        line_color = '#00ff88'
    else:
        line_color = '#ff4444'

    # Build the title text
    if current_stock['change_percent'] >= 0:
        change_text = "+" + str(current_stock['change_percent']) + "%"
    else:
        change_text = str(current_stock['change_percent']) + "%"

    chart_title = (selected_stock +
                   "   Rs." + str(current_stock['ltp']) +
                   "   (" + change_text + ")" +
                   "   H:" + str(current_stock['high']) +
                   "   L:" + str(current_stock['low']))

    # Create the line chart
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x = time_list,
        y = price_list,
        mode = 'lines',
        name = selected_stock,
        line = {'color': line_color, 'width': 2}
    ))

    # Add a dashed horizontal line at today's open price for reference
    fig.add_hline(
        y = current_stock['open'],
        line_dash = 'dash',
        line_color = '#555555',
        annotation_text = 'Open: Rs.' + str(current_stock['open']),
        annotation_font_color = '#888888'
    )

    fig.update_layout(
        title = chart_title,
        paper_bgcolor = '#0d2137',
        plot_bgcolor = '#0a0a1a',
        font = {'color': 'white'},
        xaxis= {'title': 'Time (IST)', 'gridcolor': '#1a1a2e'},
        yaxis= {'title': 'Price (Rs.)', 'gridcolor': '#1a1a2e'},
        showlegend= False,
        margin= {'l': 60, 'r': 20, 't': 50, 'b': 50}
    )

    return fig


#  Callback 5: Update live prices table 
@app.callback(
    Output('live-prices-table', 'children'),
    Input('auto-refresh', 'n_intervals')
)
def update_prices_table(n):
    if len(latest_prices) == 0:
        return html.P("Waiting for live prices...", style={'color': '#888', 'padding': '20px'})

    # Sort stocks by change percent (biggest gainers first)
    all_stocks = list(latest_prices.values())
    sorted_stocks = sorted(all_stocks, key=lambda x: x['change_percent'], reverse=True)

    # Build table rows
    all_rows = []

    # Header row
    header_row = html.Tr(children=[
        html.Th("Symbol", style={'color': '#aaaaaa', 'padding': '8px 12px', 'textAlign': 'left',  'fontSize': '12px'}),
        html.Th("LTP", style={'color': '#aaaaaa', 'padding': '8px 12px', 'textAlign': 'right', 'fontSize': '12px'}),
        html.Th("Change", style={'color': '#aaaaaa', 'padding': '8px 12px', 'textAlign': 'right', 'fontSize': '12px'}),
        html.Th("Change %", style={'color': '#aaaaaa', 'padding': '8px 12px', 'textAlign': 'right', 'fontSize': '12px'}),
        html.Th("Day High", style={'color': '#aaaaaa', 'padding': '8px 12px', 'textAlign': 'right', 'fontSize': '12px'}),
        html.Th("Day Low", style={'color': '#aaaaaa', 'padding': '8px 12px', 'textAlign': 'right', 'fontSize': '12px'}),
        html.Th("Volume", style={'color': '#aaaaaa', 'padding': '8px 12px', 'textAlign': 'right', 'fontSize': '12px'}),
    ])
    all_rows.append(header_row)

    # One row per stock
    for stock in sorted_stocks:

        # Green color for positive change, red for negative, white for zero
        if stock['change_percent'] > 0:
            change_color = '#00ff88'
            sign = '+'
        elif stock['change_percent'] < 0:
            change_color = '#ff4444'
            sign= ''
        else:
            change_color = 'white'
            sign = ''

        data_row = html.Tr(children=[
            html.Td(stock['symbol'],
                    style={'color': 'white', 'padding': '7px 12px', 'fontWeight': 'bold'}),
            html.Td("Rs." + str(stock['ltp']),
                    style={'color': 'white', 'padding': '7px 12px', 'textAlign': 'right'}),
            html.Td(sign + str(stock['change']),
                    style={'color': change_color, 'padding': '7px 12px', 'textAlign': 'right'}),
            html.Td(sign + str(stock['change_percent']) + "%",
                    style={'color': change_color, 'padding': '7px 12px', 'textAlign': 'right', 'fontWeight': 'bold'}),
            html.Td("Rs." + str(stock['high']),
                    style={'color': '#ffaa00', 'padding': '7px 12px', 'textAlign': 'right'}),
            html.Td("Rs." + str(stock['low']),
                    style={'color': '#ff8888', 'padding': '7px 12px', 'textAlign': 'right'}),
            html.Td(str(stock['volume']),
                    style={'color': '#888888', 'padding': '7px 12px', 'textAlign': 'right'}),
        ], style={'borderBottom': '1px solid #1a1a2e'})

        all_rows.append(data_row)

    return html.Table(
        children = all_rows,
        style = {'width': '100%', 'borderCollapse': 'collapse'}
    )


# Callback 6: End of Day performance bar chart 
# During market hours: shows current intraday performance of all 50 stocks
# After market closes: shows final end of day performance
# Both use the same bar chart, only the title changes
@app.callback(
    Output('eod-title','children'),
    Output('eod-performance-chart','figure'),
    Input('auto-refresh', 'n_intervals')
)
def update_eod_chart(n):
    # Create empty chart if no data
    empty_fig = go.Figure()
    empty_fig.update_layout(
        paper_bgcolor = '#0d2137',
        plot_bgcolor  = '#0a0a1a',
        font = {'color': 'white'},
        title = 'Waiting for data...'
    )

    if len(latest_prices) == 0:
        return "Performance Chart", empty_fig

    all_stocks = list(latest_prices.values())

    # Sort stocks by change percent for the bar chart
    sorted_stocks = sorted(all_stocks, key=lambda x: x['change_percent'], reverse=True)

    symbol_names= []
    change_values= []
    bar_colors = []

    for stock in sorted_stocks:
        symbol_names.append(stock['symbol'])
        change_values.append(stock['change_percent'])

        # Green bar for gainers, red bar for losers
        if stock['change_percent'] >= 0:
            bar_colors.append('#00ff88')
        else:
            bar_colors.append('#ff4444')

    # Create bar chart
    fig = go.Figure()

    fig.add_trace(go.Bar(
        x = symbol_names,
        y= change_values,
        marker_color= bar_colors,
        text= [str(v) + "%" for v in change_values],
        textposition = 'outside',
        textfont = {'color': 'white', 'size': 10}
    ))

    # Add a horizontal line at 0% for easy reference
    fig.add_hline(
        y= 0,
        line_color = '#555555',
        line_width = 1
    )

    fig.update_layout(
        paper_bgcolor = '#0d2137',
        plot_bgcolor = '#0a0a1a',
        font = {'color': 'white'},
        xaxis = {
            'title': 'Stock Symbol',
            'tickangle': -45,
            'gridcolor': '#1a1a2e',
            'tickfont': {'size': 10}
        },
        yaxis = {
            'title': 'Change from Open (%)',
            'gridcolor': '#1a1a2e',
            'zeroline': True,
            'zerolinecolor': '#555555'
        },
        showlegend= False,
        margin = {'l': 60, 'r': 20, 't': 30, 'b': 120}
    )

    # Change title based on market status
    if is_market_open():
        chart_title = "Nifty 50 — Intraday Performance (Live)"
    else:
        chart_title = "Nifty 50 — End of Day Final Performance"

    return chart_title, fig


#  Start the dashboard 
if __name__ == '__main__':
    print("Starting Dashboard...")
    print("Open Brave browser and go to: http://localhost:8050")
    print("Dashboard refreshes every 2 seconds automatically")
    print("Press Ctrl+C to stop")

    # debug=False for stable production use
    # use_reloader=False because we have a background thread running
    # host='0.0.0.0' means accessible from any device on your network
    app.run(debug=False, host='0.0.0.0', port=8050, use_reloader=False)