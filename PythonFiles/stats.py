import pandas as pd
import pytz
from itertools import combinations
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori, association_rules
import plotly.express as px
import plotly.io as pio
import plotly.graph_objects as go
from collections import Counter


def analyze_pairs(df):
    # Initialize the counter for item pairs
    item_pairs = Counter()

    # Group the DataFrame by 'order_id' to get all items in each order
    grouped_orders = df.groupby('order_id')

    # Loop through each order and get the item names
    for order_id, group in grouped_orders:
        # Get the list of items in this order
        items_in_order = group['item_name'].dropna().tolist()  # Drop NaN values (if any)

        # Ensure there are at least two items to form a pair
        if len(items_in_order) >= 2:
            # Get all unique combinations of two items in the order
            for pair in combinations(set(items_in_order), 2):
                item_pairs[pair] += 1

    # Convert item pairs counter to a DataFrame for easier manipulation
    pairs_df = pd.DataFrame(item_pairs.items(), columns=['pair', 'count'])

    # Sort pairs by count in descending order
    pairs_df = pairs_df.sort_values(by='count', ascending=False)

    return pairs_df


def ensure_latte_first(pair):
    item1, item2 = pair
    # If 'Latte' is in the pair and it's not the first item, swap them
    if item2 == 'Latte':
        return (item2, item1)
    return pair


def contains_latte(pair):
    item1, item2 = pair
    # Return True if 'Latte' is in the pair, False otherwise
    return 'Latte' in (item1, item2)


def split_latte_pairs(df):
    # Filter to get pairs that contain 'Latte'
    latte_df = df[df['pair'].apply(contains_latte)]

    # Filter to get pairs that do not contain 'Latte'
    non_latte_df = df[~df['pair'].apply(contains_latte)]

    # Return both DataFrames
    return latte_df, non_latte_df


def analyze_time_of_day(df):
    # Convert 'created_at' to Eastern Time (handles daylight savings time automatically)
    df['created_at_et'] = df['created_at'].dt.tz_convert('US/Eastern')

    # Extract the hour and minute, and combine them into a new 'time_et' column
    df['time_et'] = df['created_at_et'].dt.strftime('%H:%M')  # Combines hour and minute into 'HH:MM'

    # Extract the day of the week for analysis
    df['day_of_week'] = df['created_at_et'].dt.day_name()  # Gets day names like 'Monday', 'Tuesday', etc.

    # Extract the year for filtering and comparison
    df['year_et'] = df['created_at_et'].dt.year

    # Group by time only
    purchases_by_time = df.groupby('time_et').size()

    # Group by day of week and time (hour + minute)
    purchases_td = df.groupby(['day_of_week', 'time_et']).size().reset_index(name='count')

    # Group by day of week and time (hour + minute)
    purchases_tdy = df.groupby(['year_et', 'day_of_week', 'time_et']).size().reset_index(name='count')

    return purchases_by_time, purchases_td, purchases_tdy


def plot_pairs_table(df):

    df = df.nlargest(15, 'count')

    header_values = list(df.columns)
    cell_values = [df['pair'].tolist(), df['count'].tolist()]

    # Create a table figure
    fig = go.Figure(data=[go.Table(
        header=dict(values=header_values),
        cells=dict(values=cell_values)
    )])

    # Display the table
    fig.show()


def plot_pairs_single_bar(df, item_name):
    def remove_item_name(pair):
        item1, item2 = pair
        if item1 == item_name:
            return item2
        else:
            return item1

    df['paired_item'] = df['pair'].apply(remove_item_name)

    df_top_ten = df.nlargest(10, 'count')

    # Simple
    fig = px.bar(df_top_ten,
                 x='paired_item',
                 y='count',
                 title=f'Top 10 Items Paired with {item_name}',
                 labels={'paired_item': 'Paired Item', 'count': 'Frequency'},
                 text='count')

    fig.show()

    # Complex
    # Create a bar chart using Plotly Express for the top 10 items
    fig = px.bar(df_top_ten,
                 x='paired_item',
                 y='count',
                 title='Top 10 Items Paired with Latte',
                 labels={'paired_item': 'Paired Item', 'count': 'Frequency'},
                 text='count',
                 color='count',  # Use 'count' for color intensity
                 color_continuous_scale='Blues')  # Color gradient

    # Improve the layout and appearance
    fig.update_layout(
        title_font=dict(size=24, family='Arial', color='darkblue'),  # Customize the title font
        xaxis_title_font=dict(size=18, family='Arial', color='darkblue'),
        yaxis_title_font=dict(size=18, family='Arial', color='darkblue'),
        title_x=0.5,  # Center the title
        plot_bgcolor='rgba(0, 0, 0, 0)',  # Transparent background
        paper_bgcolor='rgba(0, 0, 0, 0)',  # Transparent background
        font=dict(size=14),  # Increase font size for axis labels
        margin=dict(l=40, r=40, t=60, b=40),  # Adjust margins for a cleaner look
    )

    # Customize hover information
    fig.update_traces(
        hovertemplate='<b>%{x}</b><br>Frequency: %{y}<extra></extra>',  # Custom hover format
        marker=dict(line=dict(color='black', width=1))  # Add border to bars
    )

    fig.show()

def plot_pairs_heatmap(df):
    # Split the 'pair' column into 'item1' and 'item2'
    df = df.head(50)
    df[['item1', 'item2']] = pd.DataFrame(df['pair'].tolist(), index=df.index)

    # Pivot the data to create a matrix of pair frequencies
    pair_matrix = df.pivot_table(index="item1", columns="item2", values="count", fill_value=0)

    # Plot a heatmap using Plotly Express
    fig = px.imshow(pair_matrix,
                    labels=dict(x="Item 2", y="Item 1", color="Frequency"),
                    x=pair_matrix.columns,
                    y=pair_matrix.index,
                    color_continuous_scale="Blues")

    # Update the layout
    fig.update_layout(title="Heatmap of Item Pair Frequencies", xaxis_nticks=36)
    fig.show()

    # New
    # Plot a heatmap using Plotly Express
    fig = px.imshow(pair_matrix,
                    labels=dict(x="Item 2", y="Item 1", color="Frequency"),
                    x=pair_matrix.columns,
                    y=pair_matrix.index,
                    color_continuous_scale="Blues")

    # Adjust the color bar's height using update_traces
    fig.update_traces(colorbar=dict(
        thickness=15,  # Adjust the thickness of the color bar
        tickfont=dict(size=10)))  # Adjust the font size of the ticks on the color bar

    # Update the layout for width and height to make it half-screen size
    fig.update_layout(
        title="Heatmap of Item Pair Frequencies",
        xaxis_nticks=36,
        width=1000,  # Set width (adjust as needed)
        height=600,  # Set height (adjust as needed)
        margin=dict(l=20, r=20, t=50, b=20)  # Adjust margins to reduce padding
    )

    # Show the plot
    fig.show()


def plot_daily_time(df):
    # Day and time
    fig = px.violin(df,
                    x='time_et',
                    y='day_of_week',
                    orientation='h',  # Horizontal ridgeline-like plot
                    color='day_of_week',
                    hover_data=['count'],  # Add hover info
                    title='Purchases by Time (HH:MM) and Day of the Week')

    # Show the plot
    fig.show()

def plot_yearly_time(df):
    df = df[time_day_year_df['year_et'].isin([2023, 2024])]

    # Create the figure object
    fig = go.Figure()

    # Add trace for 2023 (side='negative' for split violin)
    fig.add_trace(go.Violin(
        x=df['day_of_week'][df['year_et'] == 2023],
        y=df['time_et'][df['year_et'] == 2023],
        legendgroup='2023', scalegroup='2023', name='2023',
        side='negative',
        line_color='blue',  # Color for 2023
        hovertext=df['count'][df['year_et'] == 2023],  # Add hover info
    ))

    # Add trace for 2024 (side='positive' for split violin)
    fig.add_trace(go.Violin(
        x=df['day_of_week'][df['year_et'] == 2024],
        y=df['time_et'][df['year_et'] == 2024],
        legendgroup='2024', scalegroup='2024', name='2024',
        side='positive',
        line_color='orange',  # Color for 2024
        hovertext=df['count'][df['year_et'] == 2024],  # Add hover info
    ))

    # Update traces to show the mean line and make adjustments to the layout
    fig.update_traces(meanline_visible=True)

    # Layout for the plot
    fig.update_layout(
        title='Purchases by Time (HH:MM) and Day of the Week: 2023 vs 2024',
        violingap=0, violinmode='overlay',  # Overlay violins
        xaxis_title="Day of the Week",
        yaxis_title="Time (HH:MM)",
    )

    # Show the plot
    fig.show()

orders_df = pd.read_csv(r'C:\Users\samea\PycharmProjects\WaypointCoffeeSquare\orders_90k_2024.csv')

orders_df['created_at'] = pd.to_datetime(orders_df['created_at'], format='ISO8601', utc=True, errors='coerce')

##### Pairs #####
pairs_df = analyze_pairs(orders_df)
pairs_df['pair'] = pairs_df['pair'].apply(ensure_latte_first)
print(pairs_df.head(10))
pairs_df_latte, pairs_df_non_latte = split_latte_pairs(pairs_df)
print(pairs_df_latte.head(10))
print(pairs_df_non_latte.head(10))

##### Time of Day #####
(_, time_day_df, time_day_year_df) = analyze_time_of_day(orders_df)

pass

##### Plotting #####
plot_pairs_single_bar(pairs_df_latte, 'Latte')
plot_pairs_heatmap(pairs_df_non_latte)
plot_daily_time(time_day_df)
plot_yearly_time(time_day_year_df)

## New ##

