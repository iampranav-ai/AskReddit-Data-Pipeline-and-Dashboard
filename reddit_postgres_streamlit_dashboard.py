import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
from psycopg2.extras import RealDictCursor
import time
from datetime import datetime, timedelta
from collections import Counter
import re

#database connection parameters
DB_PARAMS = {
    'dbname': 'airflowPersonalData',
    'user': 'airflow',
    'password': 'airflow',
    'host': 'postgres',
    'port': '5432'
}

# function to fetch data from PostgreSQL
def fetch_data():
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM reddit_api_askReddit ORDER BY created_at DESC")
    data = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(data)

#function to preprocess data
def preprocess_data(df):
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['hour'] = df['created_at'].dt.hour
    df['day'] = df['created_at'].dt.day_name()
    df['time_of_day'] = pd.cut(df['hour'], 
                               bins=[0, 6, 12, 18, 24], 
                               labels=['Night', 'Morning', 'Afternoon', 'Evening'],
                               include_lowest=True)
    return df

# streamlit app
st.set_page_config(page_title="Reddit AskReddit Dashboard", layout="wide")

st.title("Reddit AskReddit Dashboard")

st.markdown("Made by Pranav Verma, auto refresh 15 secs, Getting data from Postgres")

# Add refresh button
if st.button("Refresh Data"):
    st.rerun()

# Fetch and preprocess data
df = fetch_data()
df = preprocess_data(df)

# Custom CSS for KPI cards
st.markdown("""
<style>
    .kpi-card {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 20px;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .kpi-value {
        font-size: 24px;
        font-weight: bold;
        color: #1f77b4;
    }
    .kpi-label {
        font-size: 16px;
        color: #555;
    }
</style>
""", unsafe_allow_html=True)

# KPIs
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown("""
    <div class="kpi-card">
        <div class="kpi-value">{}</div>
        <div class="kpi-label">Total Posts</div>
    </div>
    """.format(len(df)), unsafe_allow_html=True)

with col2:
    avg_word_count = df['word_count'].mean()
    st.markdown("""
    <div class="kpi-card">
        <div class="kpi-value">{:.2f}</div>
        <div class="kpi-label">Avg Word Count</div>
    </div>
    """.format(avg_word_count), unsafe_allow_html=True)

with col3:
    avg_char_count = df['char_count'].mean()
    st.markdown("""
    <div class="kpi-card">
        <div class="kpi-value">{:.2f}</div>
        <div class="kpi-label">Avg Char Count</div>
    </div>
    """.format(avg_char_count), unsafe_allow_html=True)

with col4:
    latest_post = df['created_at'].max()
    st.markdown("""
    <div class="kpi-card">
        <div class="kpi-value">{}</div>
        <div class="kpi-label">Latest Post</div>
    </div>
    """.format(latest_post.strftime("%Y-%m-%d %H:%M:%S")), unsafe_allow_html=True)

# Pie Chart and Donut Chart in one line
col1, col2 = st.columns(2)

with col1:
    # Pie Chart: Distribution of posts by time of day
    time_of_day_counts = df['time_of_day'].value_counts()
    fig_pie = px.pie(values=time_of_day_counts.values, names=time_of_day_counts.index, 
                     title="Distribution of Posts by Time of Day",
                     color_discrete_sequence=px.colors.sequential.Blues_r)
    st.plotly_chart(fig_pie)

with col2:
    # Donut Chart: Word Count Distribution
    word_count_bins = pd.cut(df['word_count'], bins=[0, 5, 10, 15, 20, 25, 30], labels=['0-5', '6-10', '11-15', '16-20', '21-25', '26-30'])
    word_count_dist = word_count_bins.value_counts()
    fig_donut = px.pie(values=word_count_dist.values, names=word_count_dist.index, 
                       title="Word Count Distribution", hole=0.6,
                       color_discrete_sequence=px.colors.sequential.Blues_r)
    st.plotly_chart(fig_donut)

# Heatmap: Posts by Day and Hour
heatmap_data = df.groupby(['day', 'hour']).size().unstack(fill_value=0)
fig_heatmap = px.imshow(heatmap_data, title="Posts by Day and Hour", 
                        labels=dict(x="Hour", y="Day", color="Number of Posts"),
                        color_continuous_scale="Blues")
st.plotly_chart(fig_heatmap)



# Create two columns of equal width
col1, col2 = st.columns(2)

with col1:
    # Area Chart: Posts in the Last 2 Hours
    two_hours_ago = df['created_at'].max() - timedelta(hours=12)
    df_last_12_hours = df[df['created_at'] > two_hours_ago].sort_values('created_at')
    df_last_12_hours['cumulative_posts'] = range(1, len(df_last_12_hours) + 1)
    
    fig_area = px.area(df_last_12_hours, x='created_at', y='cumulative_posts', 
                       title="Cumulative Posts in the Last 12 Hours",
                       color_discrete_sequence=["#1f77b4"])
    
    # Adjust the layout to make it more compact
    fig_area.update_layout(
        margin=dict(l=10, r=10, t=40, b=20),
        height=400  # Set a fixed height
    )
    
    st.plotly_chart(fig_area, use_container_width=True)

with col2:
    # Latest Titles Table
    st.subheader("Latest Titles")
    latest_titles = df[['title', 'created_at']].head(10)
    latest_titles['created_at'] = latest_titles['created_at'].dt.strftime("%Y-%m-%d %H:%M:%S")
    
    # Use a more compact way to display the table
    st.dataframe(latest_titles, height=400, use_container_width=True)



# Bar + Line Chart: Top 30 Most Common Words in Titles
def extract_words(title):
    return re.findall(r'\w+', title.lower())

stop_words = set([
    'want', 'whats','which', 'seen','reddit','with', 'most', 'people', 'thing', 'ever', 'about', 'best', 'who', 'think', 'it', 'would', 'out', 'why', 'if', 'or', 'has', 'is', 'the', 'you', 'what', 'do', 'of', 'and', 'your', 's', 'that', 'to', 'a', 'in', 'for', 'are', 'how', 'on', 'have',
    'was', 'were', 'be', 'been', 'being', 'am', 'is', 'are', 'can', 'could', 'will', 'would', 'should', 'may', 'might', 'must', 'shall',
    'get', 'got', 'getting', 'make', 'made', 'making', 'take', 'took', 'taking', 'go', 'went', 'going', 'come', 'came', 'coming',
    'say', 'said', 'saying', 'see', 'saw', 'seeing', 'know', 'knew', 'knowing', 'think', 'thought', 'thinking',
    # Additional words
    'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', "you're", "you've", "you'll", "you'd", 'yours', 'yourself', 'yourselves',
    'he', 'him', 'his', 'himself', 'she', "she's", 'her', 'hers', 'herself', 'it', "it's", 'its', 'itself', 'they', 'them', 'their', 'theirs',
    'themselves', 'this', 'that', "that'll", 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had',
    'having', 'do', 'does', 'did', 'doing', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'at', 'by', 'for', 'with',
    'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out',
    'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both',
    'each', 'few', 'more', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 'can', 'will',
    'just', "don't", 'should', "should've", 'now', 'ain', 'aren', "aren't", 'couldn', "couldn't", 'didn', "didn't", 'doesn', "doesn't",
    'hadn', "hadn't", 'hasn', "hasn't", 'haven', "haven't", 'isn', "isn't", 'ma', 'mightn', "mightn't", 'mustn', "mustn't", 'needn',
    "needn't", 'shan', "shan't", 'shouldn', "shouldn't", 'wasn', "wasn't", 'weren', "weren't", 'won', "won't", 'wouldn', "wouldn't",
    'also', 'like', 'ok', 'okay', 'since', 'therefore', 'without', 'yes', 'no', 'maybe', 'well', 'yeah', 'nope', 'hey', 'hi', 'hello',
    'bye', 'goodbye', 'anyway', 'always', 'never', 'sometimes', 'often', 'usually', 'rarely', 'perhaps', 'probably', 'possibly',
    'definitely', 'indeed', 'absolutely', 'certainly', 'surely', 'obviously', 'clearly', 'simply', 'really', 'actually', 'basically',
    'generally', 'literally', 'seriously', 'totally', 'virtually', 'almost', 'nearly', 'approximately', 'about', 'around', 'roughly',
    'either', 'neither', 'whether', 'whatever', 'whoever', 'whenever', 'wherever', 'however', 'whichever', 'whom', 'whose', 'anyone',
    'everyone', 'someone', 'nobody', 'everybody', 'somebody', 'anything', 'everything', 'something', 'nothing', 'everywhere', 'somewhere',
    'anywhere', 'nowhere', 'somehow', 'otherwise', 'anyway', 'anyhow', 'besides', 'nevertheless', 'nonetheless', 'although', 'though',
    'even', 'still', 'yet', 'despite', 'notwithstanding', 'regardless', 'albeit'
])
all_words = [word for title in df['title'] for word in extract_words(title) if word not in stop_words and len(word) > 3]
word_freq = Counter(all_words)
common_words = pd.DataFrame(word_freq.most_common(30), columns=['word', 'count'])

fig = make_subplots(specs=[[{"secondary_y": True}]])

fig.add_trace(
    go.Bar(x=common_words['word'], y=common_words['count'], name="Word Count", marker_color='#1f77b4'),
    secondary_y=False,
)

fig.add_trace(
    go.Scatter(x=common_words['word'], y=common_words['count'], name="Trend", mode='lines', line=dict(color='#9ecae1')),
    secondary_y=True,
)

fig.update_layout(
    title_text="Top 30 Most Common Words in Titles",
    xaxis_title="Word",
    yaxis_title="Count",
)

st.plotly_chart(fig)

# Refresh every 15 seconds
time.sleep(15)
st.rerun()