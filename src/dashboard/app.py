import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set page config for clean dark theme
st.set_page_config(
    page_title="SkillPulse India Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply Plotly Dark Theme
import plotly.io as pio
pio.templates.default = "plotly_dark"

# Custom CSS for Streamlit dark aesthetics
st.markdown("""
    <style>
    .stApp {
        background-color: #0E1117;
    }
    </style>
    """, unsafe_allow_html=True)

@st.cache_data(ttl=3600)
def load_data(query: str):
    """Loads data from BigQuery using ADC"""
    project_id = os.getenv("BQ_PROJECT_ID", "skillpulse-india")
    client = bigquery.Client(project=project_id)
    return client.query(query).to_dataframe()

def get_bq_dataset():
    """Returns the fully qualified dataset name"""
    return f"{os.getenv('BQ_PROJECT_ID', 'skillpulse-india')}.{os.getenv('BQ_DATASET_GOLD', 'skillpulse_gold')}"

dataset = get_bq_dataset()

# Sidebar Navigation
st.sidebar.image("https://cdn-icons-png.flaticon.com/512/3048/3048122.png", width=50)
st.sidebar.title("SkillPulse India")
st.sidebar.markdown("---")
page = st.sidebar.radio("Navigation", ["Skill Demand", "Salary by Role", "Geographic Intelligence"])

# --- PAGE 1: Skill Demand ---
if page == "Skill Demand":
    st.title("📈 Skill Demand & Trends")
    st.markdown("Analyze top technical skills and their market demand.")
    
    query = f"SELECT * FROM `{dataset}.vw_skill_demand`"
    try:
        df = load_data(query)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Top 20 In-Demand Skills")
            top_20 = df.sort_values(by="demand_count", ascending=False).head(20)
            fig1 = px.bar(
                top_20, 
                x="demand_count", 
                y="skill_name", 
                orientation='h',
                color="demand_count",
                color_continuous_scale="Viridis",
                labels={"demand_count": "Job Postings", "skill_name": "Skill"}
            )
            fig1.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig1, use_container_width=True)
            
        with col2:
            st.subheader("Trending vs Declining Skills")
            st.markdown("**(Bubble size = Demand)**")
            if 'first_seen' in df.columns and 'last_seen' in df.columns:
                # Calculate active lifespan as a proxy for trend analysis
                df['days_active'] = (pd.to_datetime(df['last_seen']) - pd.to_datetime(df['first_seen'])).dt.days
                
                # Handling case where all dates might be the same right now
                df['days_active'] = df['days_active'].replace(0, 1) 
                
                fig2 = px.scatter(
                    df.head(50), 
                    x="days_active", 
                    y="demand_count", 
                    color="skill_name", 
                    size="demand_count",
                    labels={"days_active": "Days Active in Market", "demand_count": "Total Postings"},
                    hover_data=["first_seen", "last_seen"]
                )
                st.plotly_chart(fig2, use_container_width=True)
            else:
                st.info("Lifecycle data not fully populated.")
                
    except Exception as e:
        st.error(f"Error loading skill data: {e}")

# --- PAGE 2: Salary by Role ---
elif page == "Salary by Role":
    st.title("💰 Salary Intelligence")
    st.markdown("Deep dive into salary expectations across different role categories.")
    
    query = f"SELECT * FROM `{dataset}.vw_salary_by_role`"
    try:
        df = load_data(query)
        
        st.subheader("Salary Range (LPA) by Role Category")
        if not df.empty:
            df["salary_spread"] = df["avg_max_salary_lpa"] - df["avg_min_salary_lpa"]
            
            # Use Plotly Range Bar to simulate a box plot for aggregated ranges
            fig = px.bar(
                df, 
                x="role_category", 
                y="salary_spread", 
                base="avg_min_salary_lpa",
                color="total_jobs", 
                color_continuous_scale="Plasma",
                labels={
                    "salary_spread": "Salary Spread (LPA)", 
                    "role_category": "Role Category",
                    "total_jobs": "Job Count"
                },
                hover_data=["avg_min_salary_lpa", "avg_max_salary_lpa"]
            )
            fig.update_layout(yaxis_title="Salary Range (LPA)")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No salary data available.")
            
    except Exception as e:
        st.error(f"Error loading salary data: {e}")
        
# --- PAGE 3: Geographic Intelligence ---
elif page == "Geographic Intelligence":
    st.title("🗺️ City Intelligence")
    st.markdown("Job distribution and top skills across Indian Tech Hubs.")
    
    query = f"SELECT * FROM `{dataset}.vw_jobs_by_city`"
    try:
        df = load_data(query)
        
        col1, col2 = st.columns([2, 1])
        
        if not df.empty:
            with col1:
                st.subheader("Job Volume by City")
                fig = px.bar(
                    df, 
                    x="city", 
                    y="job_count", 
                    color="top_skill",
                    labels={"job_count": "Job Postings", "city": "City", "top_skill": "Most Demanded Skill"},
                    barmode='stack',
                    color_discrete_sequence=px.colors.qualitative.Pastel
                )
                fig.update_layout(xaxis={'categoryorder':'total descending'})
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.subheader("Top Skill Leaderboard")
                st.dataframe(df[["city", "top_skill", "job_count"]].set_index("city"), use_container_width=True)
        else:
            st.warning("No geographic data available.")
            
    except Exception as e:
        st.error(f"Error loading geographic data: {e}")
