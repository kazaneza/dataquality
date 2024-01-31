##################
# Import libraries
import streamlit as st
import pandas as pd
import altair as alt
import plotly.express as px
from sqlalchemy import create_engine


#####################
# Page configuration
st.set_page_config(
    page_title="DATA CLEANUP PROGRESS",
    page_icon="🧹",
    layout="wide",
    initial_sidebar_state="expanded"
)

# def custom_theme():
#     return {
#         'config': {
#             'view': {
#                 'stroke': 'transparent',  # No border around charts
#                 'fill': '#F0F2F5',  # Background color for the chart area
#             },
#             'background': '#F0F2F5',  # Background color for the entire view
#             'title': {
#                 'color': '#004399',
#             },
#             'axis': {
#                 'domainColor': '#004399',
#                 'gridColor': '#cccccc',
#                 'tickColor': '#004399',
#                 'titleColor': '#004399',
#                 'labelColor': '#004399',
#             },
#             'legend': {
#                 'titleColor': '#004399',
#                 'labelColor': '#004399',
#             },
#             'mark': {
#                 'color': '#004399',  # Default color for marks
#                 'fill': '#004399',   # Default fill color for marks
#             },
#         }
#     }

# # Register and enable the custom theme
# alt.themes.register('custom_theme', custom_theme)
# alt.themes.enable('custom_theme')

st.write("")


st.markdown("""
<h2 style='text-align: center; 
           margin-top: 20px; 
           color: #FFD700; /* Gold text for better visibility on dark backgrounds */
           font-family: "Lucida Console", Monaco, monospace; /* A font that stands out on dark backgrounds */
           font-weight: bold;
           background-color: #333; /* A slightly lighter dark background for the header for contrast */
           padding: 15px;
           border-radius: 15px;
           border: 1px solid #FFD700; /* Gold border to match the text */
           box-shadow: 0 4px 8px 0 rgba(255, 215, 0, 0.2), 0 6px 20px 0 rgba(255, 215, 0, 0.19); /* Optional: Adding some glow effect */
           '>
  DATA CLEANSING DASHBOARD
</h2>
""", unsafe_allow_html=True)



###################################################
st.markdown("""
<style>

# [data-testid="stSidebar"] {
#     background-color: #f0f2f5; 
#     text-color: #004399; 
# }

.stSidebar > div:first-child {
        background-color: #f0f2f5;
}

# .css-1d391kg {
#         width: 300px;
#         }
        
#         .stSidebar > div {
#         padding: 10px 5px;
#         }

.stSidebar .stSelectbox .css-2b097c-container {
        color: #004399;
        }

[data-testid="block-container"] {
    padding-left: 2rem;
    padding-right: 2rem;
    padding-top: 1rem;
    padding-bottom: 0rem;
    margin-bottom: -7rem;
}

[data-testid="stVerticalBlock"] {
    padding-left: 0rem;
    padding-right: 0rem;
}

[data-testid="stMetric"] {
    background-color: #393939;
    text-align: center;
    padding: 15px 0;
    background-color: #F0F2F5;
    
}

[data-testid="stMetricLabel"] {
  display: flex;
  justify-content: center;
  align-items: center;
  color: #004399;
}

[data-testid="stSidebar"] {
    color: #004399;
}

[data-testid="stMetricValue"] {
  color: #004399;
}

[data-testid="stSidebarTitle"] {
  color: #004399;
}


[data-testid="stMetricDeltaIcon-Up"] {
    position: relative;
    left: 38%;
    -webkit-transform: translateX(-50%);
    -ms-transform: translateX(-50%);
    transform: translateX(-50%);
}

[data-testid="stMetricDeltaIcon-Down"] {
    position: relative;
    left: 38%;
    -webkit-transform: translateX(-50%);
    -ms-transform: translateX(-50%);
    transform: translateX(-50%);
}

</style>
""", unsafe_allow_html=True)

###########################################################
# Load data

# Database connection function
def get_db_connection():
    conn_str = "mssql+pyodbc://DATAWAREHOUSE/REPORTING_SYSTEM_DB?driver=SQL Server&Trusted_Connection=yes"
    return create_engine(conn_str)

# Function to read data from the database
def read_data_from_db(query):
    with get_db_connection().connect() as conn:
        return pd.read_sql(query, conn)

# Function to fetch and cache data, modified to accept a query
@st.cache_data
def fetch_data(query):
    return read_data_from_db(query)


# Fetching and displaying all data from BDK_Daily_Transations
sql_query_all_error =  """ SELECT COUNT(1)
FROM STAGING.DATA_CLEANSING_Errors
WHERE Extraction_Number = (SELECT MAX(Extraction_Number) FROM STAGING.DATA_CLEANSING_Errors);
"""
sql_query_all_customer = 'select count (1) from staging.DATA_CLEANSING_CUSTOMER'
template_names = fetch_data('SELECT DISTINCT Template_Name FROM STAGING.DATA_CLEANSING_Errors')
template_name_list = template_names['Template_Name'].tolist()
df = fetch_data(sql_query_all_error)
df_customer = fetch_data(sql_query_all_customer)


error_count = fetch_data(sql_query_all_error).iloc[0, 0]  
customer_count = fetch_data(sql_query_all_customer).iloc[0, 0]


sql_query_errors = """
SELECT Column_Name, COUNT(*) as count
FROM STAGING.DATA_CLEANSING_Errors 
WHERE Extraction_Number = (SELECT MAX(Extraction_Number) 
FROM STAGING.DATA_CLEANSING_Errors)
GROUP BY Column_Name
ORDER BY count DESC 
"""
df_errors_count = fetch_data(sql_query_errors)

sql_query_errors_unique= """
SELECT COUNT(DISTINCT Error_Description)
FROM STAGING.DATA_CLEANSING_Errors
WHERE Extraction_Number = (SELECT MAX(Extraction_Number) FROM STAGING.DATA_CLEANSING_Errors);
"""
df_unique_errors_count= fetch_data(sql_query_errors_unique).iloc[0, 0]
###########################

total_number_error = """select count(1) Error_Description 
from STAGING.DATA_CLEANSING_Errors
"""
df_total_number_error_count= fetch_data(total_number_error)


############################
extraction_number = """
WITH RankedData AS (
  SELECT
    Extraction_Number,
    Column_Name,
    COUNT(*) AS Count,
    ROW_NUMBER() OVER(ORDER BY COUNT(*) DESC) AS rn
  FROM STAGING.DATA_CLEANSING_Errors
  GROUP BY Extraction_Number, Column_Name
)
SELECT
  Extraction_Number,
  Column_Name,
  Count
FROM RankedData
WHERE rn <= 10
ORDER BY Count DESC, Extraction_Number ASC, Column_Name ASC;"""

df_extraction_number = fetch_data(extraction_number)








##############################
##delta column
delta_records_with_error = """
SELECT COUNT(1)
FROM STAGING.DATA_CLEANSING_Errors
WHERE Extraction_Number = (
  SELECT MAX(Extraction_Number)
  FROM STAGING.DATA_CLEANSING_Errors
  WHERE Extraction_Number < (SELECT MAX(Extraction_Number) FROM STAGING.DATA_CLEANSING_Errors)
);
"""
df_delta_records_with_error = fetch_data(delta_records_with_error).iloc[0, 0]


# Delta for unique 
delta_unique_errors = """SELECT COUNT(DISTINCT Error_Description)
FROM STAGING.DATA_CLEANSING_Errors
WHERE Extraction_Number = (
  SELECT MAX(Extraction_Number)
  FROM STAGING.DATA_CLEANSING_Errors
  WHERE Extraction_Number < (SELECT MAX(Extraction_Number) FROM STAGING.DATA_CLEANSING_Errors)
);"""

df_delta_unique_errors = fetch_data(delta_unique_errors).iloc[0, 0]



# st.dataframe(df_unique_errors_count)


# st.dataframe(df)
# st.dataframe(df_customer)

#######################
# Sidebar

with st.sidebar:
    
    logo_path = "bank-of-kigali.webp"  # Change this to the path or URL of your logo
    st.image(logo_path, width=200, use_column_width='always')
    
    st.markdown("")
    st.markdown("")
    st.markdown("")
    st.markdown("")
    st.markdown("")
    
    # st.title(' DATA CLEANSING DASHBOARD')
    st.markdown('<h1 style="color: #004399;">DATA CLEANSING DASHBOARD</h1>', unsafe_allow_html=True)
    st.markdown("")
    st.markdown("")
    st.markdown('<p style="color: #004399; font-size: 16px; margin-bottom: 5px;">Select Template Name</p>', unsafe_allow_html=True)
    selected_template_name = st.selectbox( '',template_name_list)
    
    st.markdown('<br>' * 8, unsafe_allow_html=True)

    # Footer
    footer_html = """
    <div style="color: #004399; font-size: 0.8em; text-align: center; margin-top: 20px;">
        <hr style="margin-bottom: 5px;"/>
        <p>Done by <strong>Data Management</strong></p>
    </div>
    """
    st.markdown(footer_html, unsafe_allow_html=True)
    
    
    
    
######################################
# HEATMAP
def make_heatmap(input_df, input_y, input_x, input_color, input_color_theme):
    heatmap = alt.Chart(input_df).mark_rect().encode(
            y=alt.Y(f'{input_y}:O', axis=alt.Axis(title="Extraction_Number", titleFontSize=18, titlePadding=15, titleFontWeight=900, labelAngle=0)),
            x=alt.X(f'{input_x}:O', axis=alt.Axis(title="", titleFontSize=18, titlePadding=15, titleFontWeight=900)),
            color=alt.Color(f'max({input_color}):Q',
                            legend=None,
                            scale=alt.Scale(scheme=input_color_theme)),
            stroke=alt.value('black'),
            strokeWidth=alt.value(0.25),
        ).properties(width=900
        ).configure_axis(
        labelFontSize=12,
        titleFontSize=12
        ) 
    # height=300
    return heatmap




###################################
def format_number(number):
    return "{:,}".format(number)

###################################
#  Dashboard Main Panel

col = st.columns ((1.5, 4, 3), gap='large')
with col[0]:
    st.markdown('#### OVERVIEW')
    # Display metrics for customers and errors
    error_delta = customer_count - df_delta_records_with_error
    customer_delta = 0

    st.metric(label="CUSTOMERS", value=format_number(customer_count), delta=format_number(customer_delta))
    st.metric(label="RECORDS WITH ERRORS", value=format_number(error_count), delta=format_number(error_delta))
    
    total_error_delta = 0
    df_unique_errors_count_delta = df_unique_errors_count - df_delta_unique_errors

    st.metric(label="DISTINCT ERRORS", value=format_number(df_unique_errors_count), delta=format_number(df_unique_errors_count_delta))

############################

with col[1]:
    st.markdown('#### EXTRACTION WITH THE HIGHEST ERROR RATES')
    st.write("")
    st.write("")
    st.write("")
    st.write("")
    st.write("")
    
    chart = alt.Chart(df_extraction_number).mark_bar().encode(
    x='Extraction_Number:O',
    y='Count:Q',
    color='Column_Name:N'
    ).properties(
    width=500,
    height=300
    )

# Display the chart in Streamlit
    st.altair_chart(chart, use_container_width=True)
    
#############################

with col[2]:
    st.markdown('#### COLUMNS WITH ERRORS')

    # Convert numpy.int64 to Python int
    max_count_value = int(df_errors_count['count'].max())

    st.dataframe(df_errors_count,
                 column_order=("Column_Name", "count"),
                 hide_index=True,
                 width=None,
                 column_config={
                     "Column_Name": st.column_config.TextColumn(
                         "Column_Name",
                     ),
                     "count": st.column_config.ProgressColumn(
                         "count",
                         format="%f",
                         min_value=0,
                         max_value=max_count_value,  
                     )}
                 )

with st.expander('About', expanded=True):
        st.write(f'''
            - Data: [Data Cleanup Report](https://).
            - :orange[**OVERVIEW**]: We have a record with {error_count:,} errors in {customer_count:,} customers.
            
            ''')