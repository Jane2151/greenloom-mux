import streamlit as st
import json
import os
import sqlite3
import pdfplumber
from datetime import datetime
import requests

# ─────────────────────────────────────────────
# 1. CONFIG
# ─────────────────────────────────────────────
DB_NAME = "green_loom_production.db"
DATA_FILE = "chat_data.json"

# FIX Q3: Always resolve knowledge_base relative to THIS script's location
BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
POLICY_FOLDER = os.path.join(BASE_DIR, "knowledge_base")
os.makedirs(POLICY_FOLDER, exist_ok=True)

Z_AI_API_KEY   = "os.environ.get("Z_AI_API_KEY", "")"
CUSTOM_BASE_URL = "https://api.ilmu.ai/v1"
MODEL_NAME      = "ilmu-glm-5.1"

# ── Malaysian Carbon Tax Constants ───────────
CARBON_TAX_RATE_RM    = 15.0
FREE_ALLOWANCE_TONNES = 24_000
OFFSET_PRICE_RM       = 20.0

# Emission Factors (kg CO2e per unit)
EF = {
    "petrol_litre":    2.31,
    "diesel_litre":    2.68,
    "lpg_kg":          3.02,
    "electricity_kwh": 0.694,
    "logistics_km":    0.171,
    "solid_waste_kg":  0.5,
    "wastewater_m3":   0.42,
    "scrap_metal_kg":  1.46,
    "general_waste_kg":0.5,
}

# ── Supplier registry (Q2) ────────────────────
# Each supplier has: name, distance_km (one-way), fuel_efficiency (L/100km), mode
DEFAULT_SUPPLIERS = {
    "Supplier A — Shah Alam":   {"distance_km": 45,  "fuel_l_per_100km": 12, "mode": "truck"},
    "Supplier B — Klang":       {"distance_km": 30,  "fuel_l_per_100km": 12, "mode": "truck"},
    "Supplier C — Johor Bahru": {"distance_km": 320, "fuel_l_per_100km": 14, "mode": "truck"},
    "Supplier D — Penang":      {"distance_km": 380, "fuel_l_per_100km": 14, "mode": "truck"},
    "Manual entry":             {"distance_km": 0,   "fuel_l_per_100km": 12, "mode": "truck"},
}

# ─────────────────────────────────────────────
# 2. DATABASE INIT
# ─────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS materials (
        item TEXT, price REAL, carbon_factor REAL, hs_code TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS lca_metrics (
        material TEXT PRIMARY KEY, ghg_impact TEXT,
        energy_saving REAL, cost_advantage REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS knowledge_base (
        source TEXT, page_num INTEGER, content TEXT)''')
    # FIX Q1: carbon_submissions is its own dedicated table
    c.execute('''CREATE TABLE IF NOT EXISTS carbon_submissions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        submitted_by TEXT,
        submitted_at TEXT,
        scope1_co2e REAL,
        scope2_co2e REAL,
        scope3_co2e REAL,
        waste_co2e REAL,
        total_co2e REAL,
        taxable_co2e REAL,
        carbon_tax_rm REAL,
        offset_co2e REAL,
        net_tax_rm REAL,
        supplier_used TEXT,
        notes TEXT
    )''')
    # FIX Q2: supplier carbon log
    c.execute('''CREATE TABLE IF NOT EXISTS supplier_carbon_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        logged_at TEXT,
        supplier_name TEXT,
        trips INTEGER,
        distance_km REAL,
        diesel_litres REAL,
        co2e_kg REAL,
        cost_rm REAL
    )''')
    conn.commit()
    conn.close()

init_db()

# ─────────────────────────────────────────────
# 3. PDF HELPERS
# ─────────────────────────────────────────────
def _extract_pdf_to_db(cursor, file_path: str, file_name: str) -> int:
    pages_done = 0
    with pdfplumber.open(file_path) as pdf:
        for i, page in enumerate(pdf.pages):
            for table in page.extract_tables():
                for row in table:
                    clean = [str(cell).strip() for cell in row if cell]
                    if clean:
                        cursor.execute(
                            "INSERT INTO knowledge_base VALUES (?, ?, ?)",
                            (file_name, i + 1, " | ".join(clean)))
            text = page.extract_text()
            if text and text.strip():
                cursor.execute(
                    "INSERT INTO knowledge_base VALUES (?, ?, ?)",
                    (file_name, i + 1, text.strip()))
                pages_done += 1
    return pages_done


def upload_and_add_pdf(uploaded_file) -> str:
    # FIX Q3: always save into the absolute POLICY_FOLDER path
    save_path = os.path.join(POLICY_FOLDER, uploaded_file.name)
    with open(save_path, "wb") as f:
        f.write(uploaded_file.getbuffer())

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("DELETE FROM knowledge_base WHERE source = ?", (uploaded_file.name,))
    try:
        pages = _extract_pdf_to_db(c, save_path, uploaded_file.name)
        conn.commit()
        return f"✅ **{uploaded_file.name}** saved to `{POLICY_FOLDER}` and ingested ({pages} pages)."
    except Exception as e:
        conn.rollback()
        return f"❌ Failed: {str(e)}"
    finally:
        conn.close()


def regenerate_full_dataset() -> str:
    # FIX Q3: scan the absolute POLICY_FOLDER
    pdf_files = [f for f in os.listdir(POLICY_FOLDER) if f.lower().endswith(".pdf")]
    if not pdf_files:
        return f"⚠️ No PDF files found in `{POLICY_FOLDER}`."

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("DELETE FROM knowledge_base")
    total_pages, failed = 0, []
    for file_name in pdf_files:
        try:
            pages = _extract_pdf_to_db(c, os.path.join(POLICY_FOLDER, file_name), file_name)
            total_pages += pages
        except Exception as e:
            failed.append(f"{file_name} ({e})")
    conn.commit()
    conn.close()
    msg = f"🔄 **Dataset regenerated!** {len(pdf_files)} files, {total_pages} pages ingested.\n\nFolder: `{POLICY_FOLDER}`"
    if failed:
        msg += f"\n\n⚠️ Failed: {', '.join(failed)}"
    return msg


def extract_receipt_with_ai(pdf_file, receipt_type: str) -> dict:
    text_content = ""
    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                text_content += t + "\n"
    if not text_content.strip():
        return {"error": "Could not extract text from PDF."}

    prompt = f"""Extract numerical data from this {receipt_type} receipt.

RECEIPT TEXT:
{text_content[:3000]}

Return ONLY a JSON object (no explanation, no markdown):
- Petrol/fuel: {{"litres": <number>, "fuel_type": "petrol or diesel", "amount_rm": <number>}}
- Electricity: {{"kwh": <number>, "amount_rm": <number>, "period": "<month year>"}}
- Logistics:   {{"distance_km": <number>, "fuel_litres": <number>, "amount_rm": <number>}}"""

    try:
        resp = requests.post(
            f"{CUSTOM_BASE_URL}/chat/completions",
            headers={"Authorization": f"Bearer {Z_AI_API_KEY}", "Content-Type": "application/json"},
            json={"model": MODEL_NAME,
                  "messages": [{"role": "user", "content": prompt}],
                  "temperature": 0.1, "max_tokens": 200},
            timeout=30)
        resp.raise_for_status()
        raw = resp.json()["choices"][0]["message"]["content"]
        raw = raw.strip().replace("```json","").replace("```","").strip()
        return json.loads(raw)
    except Exception as e:
        return {"error": str(e)}

# ─────────────────────────────────────────────
# 4. SMART RETRIEVAL
# ─────────────────────────────────────────────
def get_relevant_facts(query: str) -> dict:
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    keywords = [w for w in query.lower().split() if len(w) > 3]
    materials, lca = [], []
    for kw in keywords[:5]:
        c.execute("SELECT * FROM materials WHERE LOWER(item) LIKE ?", (f'%{kw}%',))
        materials.extend(c.fetchall())
        c.execute("SELECT * FROM lca_metrics WHERE LOWER(material) LIKE ?", (f'%{kw}%',))
        lca.extend(c.fetchall())
    conn.close()
    return {
        "materials": list({str(r): r for r in materials}.values()),
        "lca":       list({str(r): r for r in lca}.values())
    }

def search_knowledge_base(query: str, limit: int = 6) -> str:
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    keywords = [w for w in query.lower().split() if len(w) > 3]
    results = []
    for kw in keywords[:5]:
        c.execute(
            "SELECT source, page_num, content FROM knowledge_base "
            "WHERE LOWER(content) LIKE ? LIMIT ?", (f'%{kw}%', limit))
        for source, page, content in c.fetchall():
            results.append(f"[{source} p.{page}] {content[:400].replace(chr(10),' ')}")
    conn.close()
    seen, unique = set(), []
    for r in results:
        if r not in seen:
            seen.add(r); unique.append(r)
    return "\n".join(unique[:limit]) or "No relevant policy data found."

# ─────────────────────────────────────────────
# 5. AI ENGINE
# ─────────────────────────────────────────────
def build_system_prompt(facts, kb_data):
    return f"""You are GreenLoom AI — a sustainability and trade-compliance analyst for a Malaysian manufacturing firm.

STRUCTURED DATA:
- Materials: {facts['materials'] or 'No records.'}
- LCA Metrics: {facts['lca'] or 'No records.'}

POLICY EXCERPTS:
{kb_data}

RULES:
1. Base answers strictly on provided data.
2. If insufficient: say "Insufficient data — please upload the relevant policy PDF."
3. CEO queries: strategic insight (cost, carbon savings, competitive edge).
4. Compliance queries: cite source and page number.
5. Be concise and professional. Use bullet points for reports."""


def run_ai_analysis(user_message: str, conversation_history: list) -> str:
    facts   = get_relevant_facts(user_message)
    kb_data = search_knowledge_base(user_message)
    system  = build_system_prompt(facts, kb_data)
    messages = [{"role": "system", "content": system}]
    for msg in conversation_history[-10:]:
        role = "user" if msg["sender"] != "AI" else "assistant"
        messages.append({"role": role, "content": msg["text"]})
    messages.append({"role": "user", "content": user_message})
    try:
        response = requests.post(
            f"{CUSTOM_BASE_URL}/chat/completions",
            headers={"Authorization": f"Bearer {Z_AI_API_KEY}", "Content-Type": "application/json"},
            json={"model": MODEL_NAME, "messages": messages, "temperature": 0.3, "max_tokens": 800},
            timeout=30)
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]
    except requests.exceptions.HTTPError:
        return f"⚠️ HTTP Error {response.status_code}: {response.text}"
    except Exception as e:
        return f"⚠️ AI Error: {str(e)}"


def get_supplier_ai_recommendation(supplier_log: list) -> str:
    """Ask AI to recommend the best supplier based on carbon + cost data."""
    if not supplier_log:
        return "No supplier data available yet."

    summary = "\n".join([
        f"- {r[0]}: {r[1]} trips, {r[2]:.0f} km total, {r[3]:.1f}L diesel, "
        f"{r[4]:.2f} kg CO₂e, RM {r[5]:.2f} logistics cost"
        for r in supplier_log
    ])

    prompt = f"""You are a sustainability procurement analyst for a Malaysian manufacturer.

SUPPLIER LOGISTICS CARBON DATA:
{summary}

Carbon tax rate: RM 15 per tonne CO₂e.

Provide a SHORT recommendation (max 150 words):
1. Which supplier has the LOWEST carbon footprint per trip?
2. Which supplier is most cost-effective when carbon tax is included?
3. One actionable suggestion to reduce logistics emissions."""

    try:
        resp = requests.post(
            f"{CUSTOM_BASE_URL}/chat/completions",
            headers={"Authorization": f"Bearer {Z_AI_API_KEY}", "Content-Type": "application/json"},
            json={"model": MODEL_NAME,
                  "messages": [{"role": "user", "content": prompt}],
                  "temperature": 0.3, "max_tokens": 300},
            timeout=30)
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]
    except Exception as e:
        return f"⚠️ AI Error: {str(e)}"

# ─────────────────────────────────────────────
# 6. CARBON TAX CALCULATOR
# ─────────────────────────────────────────────
def calculate_carbon_tax(scope1, scope2, scope3, waste, offset_t):
    total_kg  = scope1 + scope2 + scope3 + waste
    total_t   = total_kg / 1000.0
    net_t     = max(0, total_t - offset_t)
    taxable_t = max(0, net_t - FREE_ALLOWANCE_TONNES)
    tax_rm    = taxable_t * CARBON_TAX_RATE_RM
    offset_cost = offset_t * OFFSET_PRICE_RM
    return {
        "scope1_t":  scope1 / 1000,
        "scope2_t":  scope2 / 1000,
        "scope3_t":  scope3 / 1000,
        "waste_t":   waste  / 1000,
        "total_t":   total_t,
        "offset_t":  offset_t,
        "net_t":     net_t,
        "allowance_t": FREE_ALLOWANCE_TONNES,
        "taxable_t": taxable_t,
        "tax_rate":  CARBON_TAX_RATE_RM,
        "tax_rm":    tax_rm,
        "offset_cost_rm": offset_cost,
        "net_tax_rm": max(0, tax_rm - offset_cost),
    }


def save_carbon_submission(result: dict, supplier_name: str, notes: str):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("""INSERT INTO carbon_submissions
        (submitted_by, submitted_at, scope1_co2e, scope2_co2e, scope3_co2e,
         waste_co2e, total_co2e, taxable_co2e, carbon_tax_rm,
         offset_co2e, net_tax_rm, supplier_used, notes)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
        st.session_state.user, datetime.now().isoformat(),
        result["scope1_t"], result["scope2_t"],
        result["scope3_t"], result["waste_t"],
        result["total_t"],  result["taxable_t"],
        result["tax_rm"],   result["offset_t"],
        result["net_tax_rm"], supplier_name, notes
    ))
    conn.commit()
    conn.close()


def log_supplier_trip(supplier_name, trips, distance_km, diesel_litres, co2e_kg, cost_rm):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("""INSERT INTO supplier_carbon_log
        (logged_at, supplier_name, trips, distance_km, diesel_litres, co2e_kg, cost_rm)
        VALUES (?,?,?,?,?,?,?)""",
        (datetime.now().isoformat(), supplier_name, trips,
         distance_km, diesel_litres, co2e_kg, cost_rm))
    conn.commit()
    conn.close()


def get_supplier_log_summary():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("""SELECT supplier_name,
                        SUM(trips), SUM(distance_km),
                        SUM(diesel_litres), SUM(co2e_kg), SUM(cost_rm)
                 FROM supplier_carbon_log
                 GROUP BY supplier_name
                 ORDER BY SUM(co2e_kg) ASC""")
    rows = c.fetchall()
    conn.close()
    return rows


def render_carbon_calculator():
    st.title("🌿 Carbon Tax Calculator")
    st.caption("Malaysian Carbon Capture, Utilisation and Storage Act 2025 — RM 15/tonne CO₂e")

    if st.button("← Back to Chat"):
        st.session_state.show_carbon = False
        st.rerun()

    st.divider()

    # ── Session state for extracted values ───────
    for k, v in [("s1_petrol_l", 0.0), ("s1_diesel_l", 0.0),
                 ("s2_kwh", 0.0), ("s3_km", 0.0), ("s3_log_fuel", 0.0)]:
        if k not in st.session_state:
            st.session_state[k] = v

    # ════════════════════════════════════════
    # SCOPE 1 — Direct (Fuel)
    # ════════════════════════════════════════
    with st.expander("🔴 SCOPE 1 — Direct Emissions (Company Vehicles & Fuel)", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            petrol_pdf = st.file_uploader("🧾 Petrol Receipt (PDF)", type="pdf", key="petrol_pdf")
            if st.button("Extract Petrol Receipt", key="ext_petrol"):
                if petrol_pdf:
                    with st.spinner("Extracting..."):
                        ext = extract_receipt_with_ai(petrol_pdf, "petrol fuel")
                    if "error" not in ext:
                        st.session_state.s1_petrol_l = float(ext.get("litres") or 0)
                        st.success(f"✅ {st.session_state.s1_petrol_l}L extracted")
                    else:
                        st.warning(f"Auto-extract failed: {ext['error']}. Enter manually.")
                else:
                    st.warning("Upload PDF first.")
        with col2:
            diesel_pdf = st.file_uploader("🧾 Diesel Receipt (PDF)", type="pdf", key="diesel_pdf")
            if st.button("Extract Diesel Receipt", key="ext_diesel"):
                if diesel_pdf:
                    with st.spinner("Extracting..."):
                        ext = extract_receipt_with_ai(diesel_pdf, "diesel fuel")
                    if "error" not in ext:
                        st.session_state.s1_diesel_l = float(ext.get("litres") or 0)
                        st.success(f"✅ {st.session_state.s1_diesel_l}L extracted")
                    else:
                        st.warning("Auto-extract failed. Enter manually.")
                else:
                    st.warning("Upload PDF first.")

        c1, c2, c3 = st.columns(3)
        with c1:
            petrol_l = st.number_input("Petrol (litres)", min_value=0.0,
                value=st.session_state.s1_petrol_l, step=1.0, key="petrol_l_in")
        with c2:
            diesel_l = st.number_input("Diesel (litres)", min_value=0.0,
                value=st.session_state.s1_diesel_l, step=1.0, key="diesel_l_in")
        with c3:
            lpg_kg = st.number_input("LPG (kg)", min_value=0.0, value=0.0, step=1.0)

        s1_kg = petrol_l*EF["petrol_litre"] + diesel_l*EF["diesel_litre"] + lpg_kg*EF["lpg_kg"]
        st.info(f"**Scope 1 Subtotal: {s1_kg/1000:.4f} t CO₂e**")

    # ════════════════════════════════════════
    # SCOPE 2 — Electricity
    # ════════════════════════════════════════
    with st.expander("🟡 SCOPE 2 — Indirect Emissions (Electricity)", expanded=True):
        elec_pdf = st.file_uploader("🧾 Electricity Bill (PDF)", type="pdf", key="elec_pdf")
        if st.button("Extract Electricity Bill", key="ext_elec"):
            if elec_pdf:
                with st.spinner("Extracting..."):
                    ext = extract_receipt_with_ai(elec_pdf, "electricity bill")
                if "error" not in ext:
                    st.session_state.s2_kwh = float(ext.get("kwh") or 0)
                    st.success(f"✅ {st.session_state.s2_kwh} kWh extracted")
                else:
                    st.warning("Auto-extract failed. Enter manually.")
            else:
                st.warning("Upload PDF first.")

        kwh = st.number_input("Electricity consumed (kWh)", min_value=0.0,
            value=st.session_state.s2_kwh, step=10.0, key="kwh_in")
        s2_kg = kwh * EF["electricity_kwh"]
        st.info(f"**Scope 2 Subtotal: {s2_kg/1000:.4f} t CO₂e**  "
                f"(Malaysia grid: {EF['electricity_kwh']} kg CO₂e/kWh)")

    # ════════════════════════════════════════
    # SCOPE 3 — Logistics + Supplier Decision
    # ════════════════════════════════════════
    with st.expander("🟠 SCOPE 3 — Logistics & Supplier Carbon Decision (Q2 Fix)", expanded=True):
        st.markdown("**Select your supplier to auto-fill distance, or enter manually.**")

        # FIX Q2: Supplier selector
        supplier_choice = st.selectbox(
            "Select Supplier / Route", list(DEFAULT_SUPPLIERS.keys()), key="supplier_sel")
        supplier_data = DEFAULT_SUPPLIERS[supplier_choice]

        trips = st.number_input("Number of trips (return = 2×)", min_value=1, value=1, step=1)

        if supplier_choice != "Manual entry":
            auto_km   = supplier_data["distance_km"] * trips
            auto_fuel = round(auto_km * supplier_data["fuel_l_per_100km"] / 100, 1)
            st.caption(f"Auto-filled: {supplier_data['distance_km']} km × {trips} trips "
                       f"= **{auto_km} km**, estimated **{auto_fuel}L diesel**")
        else:
            auto_km   = 0.0
            auto_fuel = 0.0

        # Receipt upload
        log_pdf = st.file_uploader("🧾 Logistics Receipt (PDF, optional)", type="pdf", key="log_pdf")
        if st.button("Extract Logistics Receipt", key="ext_log"):
            if log_pdf:
                with st.spinner("Extracting..."):
                    ext = extract_receipt_with_ai(log_pdf, "logistics transport")
                if "error" not in ext:
                    st.session_state.s3_km       = float(ext.get("distance_km") or auto_km)
                    st.session_state.s3_log_fuel = float(ext.get("fuel_litres") or auto_fuel)
                    st.success(f"✅ {st.session_state.s3_km} km, {st.session_state.s3_log_fuel}L extracted")
                else:
                    st.warning("Auto-extract failed. Using supplier auto-fill.")
                    st.session_state.s3_km       = auto_km
                    st.session_state.s3_log_fuel = auto_fuel
            else:
                st.session_state.s3_km       = auto_km
                st.session_state.s3_log_fuel = auto_fuel

        c1, c2 = st.columns(2)
        with c1:
            log_km = st.number_input("Total logistics distance (km)", min_value=0.0,
                value=float(st.session_state.s3_km or auto_km), step=10.0, key="log_km_in")
        with c2:
            log_diesel = st.number_input("Logistics diesel (litres)", min_value=0.0,
                value=float(st.session_state.s3_log_fuel or auto_fuel), step=1.0, key="log_diesel_in")

        s3_kg = log_km*EF["logistics_km"] + log_diesel*EF["diesel_litre"]
        s3_tax_cost = (s3_kg/1000) * CARBON_TAX_RATE_RM

        st.info(f"**Scope 3 Subtotal: {s3_kg/1000:.4f} t CO₂e**  |  "
                f"Estimated carbon tax cost: **RM {s3_tax_cost:.2f}**")

        # Log this supplier trip
        if st.button("📊 Log This Supplier Trip for Comparison", key="log_supplier"):
            log_supplier_trip(
                supplier_choice, trips, log_km, log_diesel,
                s3_kg, s3_tax_cost
            )
            st.success(f"✅ Trip logged for {supplier_choice}")

        # Supplier comparison table
        st.divider()
        st.subheader("📊 Supplier Carbon Comparison")
        log_rows = get_supplier_log_summary()
        if log_rows:
            st.table([{
                "Supplier":        r[0],
                "Total Trips":     int(r[1]),
                "Distance (km)":   f"{r[2]:.0f}",
                "Diesel (L)":      f"{r[3]:.1f}",
                "CO₂e (kg)":       f"{r[4]:.2f}",
                "Carbon Cost (RM)":f"{r[5]:.2f}",
            } for r in log_rows])

            if st.button("🤖 Get AI Supplier Recommendation", key="ai_supplier"):
                with st.spinner("Analysing suppliers..."):
                    rec = get_supplier_ai_recommendation(log_rows)
                st.markdown("**🤖 AI Recommendation:**")
                st.success(rec)
        else:
            st.caption("No supplier trips logged yet. Log a trip above to start comparing.")

    # ════════════════════════════════════════
    # WASTE
    # ════════════════════════════════════════
    with st.expander("⚫ WASTE EMISSIONS (All Types)", expanded=True):
        st.caption("Emission factors: Solid 0.5 | Wastewater 0.42/m³ | Scrap metal 1.46 | General 0.5 kg CO₂e/kg")
        c1, c2 = st.columns(2)
        with c1:
            solid_kg   = st.number_input("🗑️ Solid waste (kg)",           min_value=0.0, step=1.0, key="sw")
            scrap_kg   = st.number_input("🔩 Scrap / off-cut metal (kg)", min_value=0.0, step=1.0, key="sm")
        with c2:
            ww_m3      = st.number_input("💧 Wastewater (m³)",            min_value=0.0, step=0.1, key="ww")
            general_kg = st.number_input("📦 General / other waste (kg)", min_value=0.0, step=1.0, key="gw")

        st.text_area("📝 Other waste (describe — for record keeping)",
            placeholder="e.g. Chemical solvent 20L, Packaging foam 50kg …", key="custom_waste")

        waste_kg = (solid_kg*EF["solid_waste_kg"] + ww_m3*EF["wastewater_m3"] +
                    scrap_kg*EF["scrap_metal_kg"] + general_kg*EF["general_waste_kg"])
        st.info(f"**Waste Subtotal: {waste_kg/1000:.4f} t CO₂e**")

    # ════════════════════════════════════════
    # OFFSETS
    # ════════════════════════════════════════
    with st.expander("🌱 Carbon Offsets (Optional)", expanded=False):
        st.markdown(f"Offset cost assumed at **RM {OFFSET_PRICE_RM}/tonne** (voluntary market).")
        offset_t = st.number_input("Carbon credits purchased (tonnes CO₂e)",
                                    min_value=0.0, step=1.0, key="offset_t")

    # ════════════════════════════════════════
    # CALCULATE
    # ════════════════════════════════════════
    st.divider()
    notes_input = st.text_input("📝 Submission notes (optional)",
                                 placeholder="e.g. Q1 2025 — includes logistics to Supplier B")

    if st.button("⚡ Calculate Carbon Tax", type="primary", use_container_width=True):
        result = calculate_carbon_tax(s1_kg, s2_kg, s3_kg, waste_kg, offset_t)
        st.session_state["carbon_result"]   = result
        st.session_state["carbon_supplier"] = supplier_choice
        st.session_state["carbon_notes"]    = notes_input

    # ════════════════════════════════════════
    # RESULTS
    # ════════════════════════════════════════
    if "carbon_result" in st.session_state:
        r = st.session_state["carbon_result"]
        st.divider()
        st.subheader("📊 Carbon Tax Breakdown")

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("🔴 Scope 1", f"{r['scope1_t']:.3f} t")
        c2.metric("🟡 Scope 2", f"{r['scope2_t']:.3f} t")
        c3.metric("🟠 Scope 3", f"{r['scope3_t']:.3f} t")
        c4.metric("⚫ Waste",   f"{r['waste_t']:.3f} t")
        st.markdown("---")

        c5, c6, c7 = st.columns(3)
        c5.metric("📦 Total Emissions", f"{r['total_t']:.3f} t CO₂e")
        c6.metric("🌱 Offsets Applied", f"− {r['offset_t']:.3f} t")
        c7.metric("📉 Net Emissions",   f"{r['net_t']:.3f} t CO₂e")
        st.markdown("---")

        c8, c9, c10 = st.columns(3)
        c8.metric("🆓 Free Allowance",   f"{r['allowance_t']:,} t")
        c9.metric("💰 Taxable Emissions",f"{r['taxable_t']:.3f} t")
        c10.metric("🏷️ Tax Rate",        f"RM {r['tax_rate']:.0f}/t")
        st.markdown("---")

        cA, cB, cC = st.columns(3)
        cA.metric("🧾 Gross Carbon Tax", f"RM {r['tax_rm']:,.2f}")
        cB.metric("🌿 Offset Cost",      f"RM {r['offset_cost_rm']:,.2f}")
        cC.metric("✅ NET TAX PAYABLE",  f"RM {r['net_tax_rm']:,.2f}",
                  delta="Below threshold" if r["taxable_t"] == 0 else None)

        with st.expander("🔍 Calculation Steps"):
            st.markdown(f"""
**Step 1 — Activity × Emission Factor (kg CO₂e):**
Scope 1: `{r['scope1_t']*1000:.2f}` | Scope 2: `{r['scope2_t']*1000:.2f}` | Scope 3: `{r['scope3_t']*1000:.2f}` | Waste: `{r['waste_t']*1000:.2f}`
→ **Total: {r['total_t']:.4f} tonnes**

**Step 2 — Deduct Offsets:** {r['total_t']:.4f} − {r['offset_t']:.4f} = **{r['net_t']:.4f} t**

**Step 3 — Free Allowance:** max(0, {r['net_t']:.4f} − {r['allowance_t']:,}) = **{r['taxable_t']:.4f} t taxable**

**Step 4 — Apply Tax Rate:** {r['taxable_t']:.4f} × RM {r['tax_rate']} = **RM {r['tax_rm']:,.2f}**

**Step 5 — Deduct Offset Cost:** RM {r['tax_rm']:,.2f} − RM {r['offset_cost_rm']:,.2f} = **RM {r['net_tax_rm']:,.2f}**
""")

        col_s, col_c = st.columns(2)
        with col_s:
            if st.button("💾 Save Submission", use_container_width=True):
                save_carbon_submission(
                    r,
                    st.session_state.get("carbon_supplier", "—"),
                    st.session_state.get("carbon_notes", "")
                )
                st.success("✅ Saved to database.")
        with col_c:
            if st.button("🗑️ Clear", use_container_width=True):
                del st.session_state["carbon_result"]
                st.rerun()

    # Past submissions (CEO only)
    if st.session_state.role == "CEO":
        st.divider()
        st.subheader("📋 Past Submissions")
        try:
            conn = sqlite3.connect(DB_NAME)
            c = conn.cursor()
            c.execute("""SELECT submitted_by, submitted_at, total_co2e,
                                taxable_co2e, net_tax_rm, supplier_used, notes
                         FROM carbon_submissions ORDER BY submitted_at DESC LIMIT 10""")
            rows = c.fetchall()
            conn.close()
            if rows:
                st.table([{
                    "By": r[0], "Date": r[1][:16],
                    "Total CO₂e (t)": f"{r[2]:.3f}",
                    "Taxable (t)":    f"{r[3]:.3f}",
                    "Net Tax (RM)":   f"{r[4]:,.2f}",
                    "Supplier":       r[5] or "—",
                    "Notes":          r[6] or "—"
                } for r in rows])
            else:
                st.caption("No submissions yet.")
        except Exception as e:
            st.caption(f"Could not load history: {e}")

# ─────────────────────────────────────────────
# 7. AUTH
# ─────────────────────────────────────────────
def load_data() -> dict:
    if os.path.exists(DATA_FILE) and os.path.getsize(DATA_FILE) > 0:
        try:
            with open(DATA_FILE, "r") as f:
                return json.load(f)
        except json.JSONDecodeError:
            pass
    return {
        "users": {
            "Yip":  {"pin": "abc123", "role": "CEO"},
            "Chow": {"pin": "IU",     "role": "Manager"}
        },
        "messages": []
    }

def save_data(d: dict):
    with open(DATA_FILE, "w") as f:
        json.dump(d, f, indent=4)

data = load_data()

# ─────────────────────────────────────────────
# 8. SESSION STATE
# ─────────────────────────────────────────────
for key, default in [
    ("user", None), ("role", None),
    ("show_report", False), ("current_report", ""),
    ("show_policy", False), ("show_carbon", False)
]:
    if key not in st.session_state:
        st.session_state[key] = default

# ─────────────────────────────────────────────
# 9. LOGIN
# ─────────────────────────────────────────────
if st.session_state.user is None:
    st.title("🏢 GreenLoom Corporate Portal")
    st.caption("Sustainability & Compliance AI Edition")
    with st.form("login_form"):
        user_choice = st.selectbox("Select Profile", list(data["users"].keys()))
        password    = st.text_input("Enter PIN", type="password")
        if st.form_submit_button("Login"):
            if data["users"][user_choice]["pin"] == password:
                st.session_state.user = user_choice
                st.session_state.role = data["users"][user_choice]["role"]
                st.rerun()
            else:
                st.error("Incorrect PIN.")
    st.stop()

# ─────────────────────────────────────────────
# 10. SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:
    st.title(f"🛠️ {st.session_state.role} Panel")
    st.caption(f"Logged in as **{st.session_state.user}**")

    if st.button("Logout", use_container_width=True):
        st.session_state.user = None
        st.session_state.role = None
        st.rerun()

    st.divider()
    # CEO + Manager can access Carbon Calculator
    if st.button("🌿 Carbon Tax Calculator", use_container_width=True):
        st.session_state.show_carbon = True
        st.session_state.show_report = False
        st.session_state.show_policy = False
        st.rerun()

    if st.session_state.role == "CEO":
        st.divider()
        st.subheader("📊 AI Reports")
        if st.button("Generate AI Report", use_container_width=True):
            with st.spinner("Analysing..."):
                history  = data["messages"][-10:]
                last_msg = next(
                    (m["text"] for m in reversed(history) if m["sender"] != "AI"),
                    "Provide a full sustainability and compliance summary.")
                st.session_state.current_report = run_ai_analysis(last_msg, history)
                st.session_state.show_report = True
                st.session_state.show_policy = False
                st.session_state.show_carbon = False
            st.rerun()

        st.divider()
        st.subheader("📂 Policy Management")
        if st.button("📥 Open Policy Upload Panel", use_container_width=True):
            st.session_state.show_policy = True
            st.session_state.show_report = False
            st.session_state.show_carbon = False
            st.rerun()

        st.divider()
        st.subheader("🗄️ DB Stats")
        try:
            conn = sqlite3.connect(DB_NAME)
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM knowledge_base")
            kb_count = c.fetchone()[0]
            c.execute("SELECT COUNT(DISTINCT source) FROM knowledge_base")
            src_count = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM carbon_submissions")
            sub_count = c.fetchone()[0]
            conn.close()
            st.metric("KB Records",         kb_count)
            st.metric("PDF Sources",        src_count)
            st.metric("Carbon Submissions", sub_count)
        except Exception:
            st.caption("DB unavailable")

# ─────────────────────────────────────────────
# 11. MAIN VIEWS
# ─────────────────────────────────────────────

# VIEW A — Carbon Calculator (CEO + Manager)
if st.session_state.show_carbon:
    render_carbon_calculator()

# VIEW B — Policy Upload (CEO only)
elif st.session_state.show_policy and st.session_state.role == "CEO":
    st.title("📂 Policy Management")
    st.caption(f"Policy folder: `{POLICY_FOLDER}`")
    if st.button("← Back to Chat"):
        st.session_state.show_policy = False
        st.rerun()
    st.divider()

    st.subheader("➕ Add / Update a Policy PDF")
    new_pdf = st.file_uploader("Upload Policy PDF", type="pdf", key="policy_uploader")
    if st.button("💾 Save & Ingest PDF", use_container_width=True):
        if new_pdf:
            with st.spinner(f"Processing {new_pdf.name}..."):
                result = upload_and_add_pdf(new_pdf)
            st.success(result)
        else:
            st.warning("Please select a PDF file first.")

    st.divider()
    st.subheader("🔄 Regenerate Full Dataset")
    pdf_list = [f for f in os.listdir(POLICY_FOLDER) if f.lower().endswith(".pdf")]
    if pdf_list:
        st.info(f"**{len(pdf_list)} PDF(s) found:** " + ", ".join(pdf_list))
    else:
        st.warning(f"No PDFs found in `{POLICY_FOLDER}`.")
    if st.button("🔄 Regenerate Full Dataset", use_container_width=True, type="primary"):
        with st.spinner("Wiping and re-extracting all PDFs..."):
            result = regenerate_full_dataset()
        st.success(result)
        st.rerun()

# VIEW C — AI Report (CEO only)
elif st.session_state.show_report and st.session_state.role == "CEO":
    st.title("📊 AI Analysis Report")
    st.caption(f"Generated at {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    if st.button("← Back to Chat"):
        st.session_state.show_report = False
        st.rerun()
    st.divider()
    st.markdown(st.session_state.current_report)

# VIEW D — Chat (default)
else:
    st.title("💬 GreenLoom Operations Chat")
    for msg in data["messages"]:
        role = "user" if msg["sender"] != "AI" else "assistant"
        with st.chat_message(role):
            label = f"**{msg['sender']}**" if msg["sender"] != "AI" else "🤖 **GreenLoom AI**"
            st.markdown(f"{label}: {msg['text']}")

    if prompt := st.chat_input("Ask about materials, carbon tax, HS codes, or compliance..."):
        data["messages"].append({
            "sender": st.session_state.user,
            "text": prompt,
            "timestamp": datetime.now().isoformat()
        })
        with st.spinner("GreenLoom AI is thinking..."):
            ai_reply = run_ai_analysis(prompt, data["messages"])
        data["messages"].append({
            "sender": "AI",
            "text": ai_reply,
            "timestamp": datetime.now().isoformat()
        })
        save_data(data)
        st.rerun()