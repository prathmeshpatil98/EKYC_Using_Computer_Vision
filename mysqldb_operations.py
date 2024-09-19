from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pandas as pd
import streamlit as st

# Retrieve connection details from secrets
db_url = (
    f"mysql+mysqlconnector://{st.secrets['mysql']['user']}:{st.secrets['mysql']['password']}"
    f"@{st.secrets['mysql']['host']}:{st.secrets['mysql']['port']}/{st.secrets['mysql']['database']}"
)

# Initialize SQLAlchemy engine and session
engine = create_engine(db_url)
Session = sessionmaker(bind=engine)

def insert_records(text_info):
    try:
        # Create a new session
        with Session() as session:
            session.execute(
                text(
                    'INSERT INTO users (id, name, father_name, dob, id_type, embedding) VALUES (:id, :name, :father_name, :dob, :id_type, :embedding);'
                ),
                {
                    'id': text_info['ID'],
                    'name': text_info['Name'],
                    'father_name': text_info["Father's Name"],
                    'dob': text_info['DOB'],  # Ensure this is formatted as 'YYYY-MM-DD'
                    'id_type': text_info['ID Type'],
                    'embedding': str(text_info['Embedding'])
                }
            )
            session.commit()
    except Exception as e:
        st.error(f"An error occurred: {e}")

def fetch_record():
    try:
        with engine.connect() as conn:
            df = pd.read_sql('SELECT * FROM users;', conn)
        return df
    except Exception as e:
        st.error(f"An error occurred: {e}")
        return pd.DataFrame()

def check_duplicacy(text_info):
    df = fetch_record()
    is_duplicate = df[df['id'] == text_info['ID']].shape[0] > 0
    return is_duplicate
