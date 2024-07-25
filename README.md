# Reddit AskReddit Data Pipeline and Dashboard

This project consists of two main components:
1. An Airflow DAG for fetching data from Reddit's AskReddit subreddit
2. A Streamlit dashboard for visualizing the collected data

## 1. Reddit Data Pipeline (reddit_data_pipeline.py)

This Airflow DAG continuously fetches new posts from the AskReddit subreddit, processes them, and stores the data in both CSV files and a PostgreSQL database.

### Features:
- Fetches new posts from Reddit's API every 5 seconds
- Processes post data (title, subreddit, creation time, word count, character count)
- Saves data to CSV files
- Stores data in a PostgreSQL database
- Handles duplicate posts to prevent redundant entries

### Requirements:
- Apache Airflow
- PostgreSQL
- Python libraries: requests, unidecode, psycopg2

### Usage:
1. Set up Airflow and PostgreSQL
2. Configure the DAG in your Airflow environment
3. Start the Airflow scheduler and webserver

## 2. Reddit PostgreSQL Streamlit Dashboard (reddit_postgres_streamlit_dashboard.py)

This Streamlit app creates an interactive dashboard to visualize the data collected by the Airflow DAG.

### Features:
- Connects to PostgreSQL database to fetch the latest data
- Displays key performance indicators (KPIs)
- Visualizes data using various charts:
  - Pie chart for post distribution by time of day
  - Donut chart for word count distribution
  - Heatmap for posts by day and hour
  - Area chart for cumulative posts in the last 12 hours
  - Bar and line chart for top 30 most common words in titles
- Displays a table of the latest post titles
- Auto-refreshes every 15 seconds

### Requirements:
- Streamlit
- PostgreSQL
- Python libraries: pandas, plotly, psycopg2

### Usage:
1. Install required libraries: `pip install streamlit pandas plotly psycopg2`
2. Run the Streamlit app: `streamlit run reddit_postgres_streamlit_dashboard.py`

## Screenshots

![reddit_airflow'](https://github.com/user-attachments/assets/f7db97ce-d4a4-41e6-8fbf-79669ef7edca)

![reddit_airflow_postgres](https://github.com/user-attachments/assets/9e517b43-283c-41e8-b63c-d89e9e4b0e31)

![reddit_airflow_postgres1](https://github.com/user-attachments/assets/decd6f02-03d2-47f1-b8ad-46deecd025b0)

![reddit_airflow_postgres2](https://github.com/user-attachments/assets/c29cb1a3-b681-4204-906f-9f6980be3dfd)


## Setup Instructions

1. Clone this repository
2. Set up Airflow and PostgreSQL
3. Configure the Airflow DAG (reddit_data_pipeline.py)
4. Start the Airflow scheduler and webserver
5. Run the Streamlit dashboard (reddit_postgres_streamlit_dashboard.py)

## Contributing

Feel free to fork this project and submit pull requests with improvements or new features.

## License

[Insert your chosen license here]

## Author

Pranav Verma
