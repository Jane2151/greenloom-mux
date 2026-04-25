import streamlit as st
import json
import os
import sqlite3
import pdfplumber
from datetime import datetime
from zhipuai import ZhipuAI
from dotenv import load_dotenv

load_dotenv()

# --- 1. CONFIG & ILMU-GLM API ---
# CRITICAL: Use the specific API Key and Base URL provided in the guide
Z_AI_API_KEY = os.environ.get("Z_AI_API_KEY", "")
CUSTOM_BASE_URL = "https://api.ilmu.ai/v4"
DB_NAME = "green_loom_production.db"

# Initialize the client specifically for the hackathon endpoint
client = ZhipuAI(
    api_key=Z_AI_API_KEY,
    base_url=CUSTOM_BASE_URL
)


# --- 2. INTEGRATED DATA EXTRACTION LOGIC ---
def process_uploaded_file(uploaded_file):
    """Redo extraction for a new policy PDF and update the knowledge base."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    # Table schema from your Data_extract.py
    c.execute('''CREATE TABLE IF NOT EXISTS knowledge_base 
                 (source TEXT, page_num INTEGER, content TEXT)''')

    with pdfplumber.open(uploaded_file) as pdf:
        for i, page in enumerate(pdf.pages):
            # Extract Tables (for HS Codes, Prices) [cite: 3, 238, 627]
            tables = page.extract_tables()
            for table in tables:
                for row in table:
                    clean_row = [str(cell).strip() for cell in row if cell]
                    if clean_row:
                        c.execute("INSERT INTO knowledge_base VALUES (?, ?, ?)",
                                  (uploaded_file.name, i + 1, " | ".join(clean_row)))
            # Extract Raw Text (for Law/Policy acts) [cite: 23, 99, 736]
            text = page.extract_text()
            if text:
                c.execute("INSERT INTO knowledge_base VALUES (?, ?, ?)",
                          (uploaded_file.name, i + 1, text.strip()))
    conn.commit()
    conn.close()
    return f"✅ Database updated with {uploaded_file.name}."


# --- 3. SQL DATA RETRIEVAL ---
def get_authentic_facts():
    """Fetches verified facts from your extracted database."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    try:
        # Columns match your Data_extract.py schema
        c.execute("SELECT item, price, carbon_factor, hs_code FROM materials")
        materials = c.fetchall()
        c.execute("SELECT material, ghg_impact, energy_saving, cost_advantage FROM lca_metrics")
        lca = c.fetchall()
    except sqlite3.OperationalError:
        conn.close()
        return None
    conn.close()
    return {"materials": materials, "lca": lca}


def run_ai_analysis(chat_history):
    """Uses ILMU-GLM-5.1 for high-precision compliance reasoning."""
    facts = get_authentic_facts()

    prompt = f"""
    Analyze this procurement chat: {chat_history}

    GROUNDING DATA (FROM SQL):
    - Materials/HS Codes: {facts['materials'] if facts else 'No data'}
    - LCA Research: {facts['lca'] if facts else 'No data'}

    TASK: Generate two reports based on the Carbon Capture, Utilization and Storage Act 2025.
    1. CEO STRATEGIC: Highlight the 4% lifecycle cost advantage for steel.
    2. GOV COMPLIANCE: Use HS Code 7216.32.000 and the RM 15/tonne tax rate.
    """

    # Updated to use the specific model required by organizers
    response = client.chat.completions.create(
        model="ilmu-glm-5.1",
        messages=[{"role": "user", "content": prompt}]
    )
    return response.choices[0].message.content


# --- 4. AUTH & SESSION ---
DATA_FILE = "chat_data.json"


def load_data():
    if os.path.exists(DATA_FILE) and os.path.getsize(DATA_FILE) > 0:
        with open(DATA_FILE, "r") as f: return json.load(f)
    return {"users": {"Yip": {"pin": "abc123", "role": "CEO"}, "Chow": {"pin": "IU", "role": "Manager"}},
            "messages": []}


data = load_data()

if "user" not in st.session_state:
    st.session_state.user = None
    st.session_state.show_report = False

if st.session_state.user is None:
    st.title("🏢 M-U-X Portal (ILMU-GLM)")
    user_choice = st.selectbox("Select Profile", list(data["users"].keys()))
    password = st.text_input("Enter Pin", type="password")
    if st.button("Login"):
        if data["users"][user_choice]["pin"] == password:
            st.session_state.user, st.session_state.role = user_choice, data["users"][user_choice]["role"]
            st.rerun()
    st.stop()

# --- 5. SIDEBAR (CEO CONTROLS) ---
with st.sidebar:
    st.title(f"🛠️ {st.session_state.role} Panel")

    if st.session_state.role == "CEO":
        st.divider()
        st.subheader("📂 Policy Live-Update")
        new_pdf = st.file_uploader("Upload New Policy PDF", type="pdf")
        if st.button("🔄 Update Knowledge Base") and new_pdf:
            with st.spinner("Processing..."):
                st.success(process_uploaded_file(new_pdf))

        st.divider()
        st.subheader("📊 Intelligence")
        st.session_state.active_branch = st.radio("Type:", ["CEO Strategic", "Gov Compliance"])
        if st.button("🚀 Generate ILMU-GLM Report"):
            history = " ".join([m["text"] for m in data["messages"][-15:]])
            st.session_state.current_report = run_ai_analysis(history)
            st.session_state.show_report = True

# --- 6. MAIN PAGE ---
if st.session_state.show_report and st.session_state.role == "CEO":
    st.title(f"📊 {st.session_state.active_branch}")
    if st.button("← Back"):
        st.session_state.show_report = False
        st.rerun()
    st.markdown("---")
    st.write(st.session_state.get("current_report"))
else:
    st.title("💬 Operations Chat")
    for msg in data["messages"]:
        with st.chat_message("user"): st.write(f"**{msg['sender']}**: {msg['text']}")

    if prompt := st.chat_input("Type a message..."):
        data["messages"].append({"sender": st.session_state.user, "text": prompt})
        with open(DATA_FILE, "w") as f: json.dump(data, f, indent=4)
        st.rerun()