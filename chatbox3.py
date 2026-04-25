import streamlit as st
import json
import os
import sqlite3
import pdfplumber
from datetime import datetime
import requests
import base64
import re
from fpdf import FPDF

# ─────────────────────────────────────────────
# 1. CONFIG
# ─────────────────────────────────────────────
DB_NAME = "green_loom_production.db"
DATA_FILE = "chat_data.json"

# FIX Q3: Always resolve knowledge_base relative to THIS script's location
BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
POLICY_FOLDER = os.path.join(BASE_DIR, "knowledge_base")
os.makedirs(POLICY_FOLDER, exist_ok=True)

SUBMISSION_REPORT_FOLDER = os.path.join(BASE_DIR, "submission_report")
os.makedirs(SUBMISSION_REPORT_FOLDER, exist_ok=True)

Z_AI_API_KEY   = os.environ.get("Z_AI_API_KEY", "")
CUSTOM_BASE_URL = "https://api.ilmu.ai/v1"
MODEL_NAME      = "ilmu-glm-5.1"

# ── Groq Vision API (for Wastage Identify) ───
GROQ_API_KEY    = os.environ.get("GROQ_API_KEY", "")
GROQ_BASE_URL   = "https://api.groq.com/openai/v1"
GROQ_VISION_MODEL = "meta-llama/llama-4-scout-17b-16e-instruct"

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
    # Drop old carbon_submissions if schema is outdated, then recreate
    c.execute("PRAGMA table_info(carbon_submissions)")
    existing_cols = {row[1] for row in c.fetchall()}
    expected_cols = {"id", "submitted_by", "submitted_at", "scope1_co2e", "scope2_co2e",
                     "scope3_co2e", "scope3_logistics_co2e", "scope3_waste_co2e",
                     "total_co2e", "taxable_co2e", "carbon_tax_rm", "offset_co2e",
                     "net_tax_rm", "supplier_used", "notes"}
    if existing_cols and not expected_cols.issubset(existing_cols):
        c.execute("DROP TABLE carbon_submissions")
    c.execute('''CREATE TABLE IF NOT EXISTS carbon_submissions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        submitted_by TEXT,
        submitted_at TEXT,
        scope1_co2e REAL,
        scope2_co2e REAL,
        scope3_co2e REAL,
        scope3_logistics_co2e REAL,
        scope3_waste_co2e REAL,
        total_co2e REAL,
        taxable_co2e REAL,
        carbon_tax_rm REAL,
        offset_co2e REAL,
        net_tax_rm REAL,
        supplier_used TEXT,
        notes TEXT
    )''')
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


def identify_wastage_with_ai(image_file) -> dict:
    """Use Groq vision AI to identify waste type and estimate weight from an uploaded image."""
    try:
        img_bytes = image_file.getvalue()
        if not img_bytes:
            img_bytes = image_file.read()
        b64_img = base64.b64encode(img_bytes).decode("utf-8")
        mime_map = {"jpg": "image/jpeg", "jpeg": "image/jpeg", "png": "image/png",
                    "webp": "image/webp", "gif": "image/gif"}
        ext = image_file.name.rsplit(".", 1)[-1].lower()
        mime = mime_map.get(ext, "image/jpeg")

        prompt = """You are a waste identification and weight estimation expert for a Malaysian manufacturing plant.

Analyze the image of waste material and return ONLY a JSON object with these fields (no explanation, no markdown):
{
  "waste_type": "One of: solid_waste, scrap_metal, general_waste, wastewater",
  "waste_type_label": "Human-readable label e.g. Solid Waste, Scrap Metal, General Waste, Wastewater",
  "estimated_weight_kg": <estimated weight in kg as a number>,
  "confidence_percent": <integer from 0 to 100 representing how confident you are>,
  "description": "Brief description of what you see",
  "material_details": "Specific materials identified",
  "estimation_reasoning": "How you arrived at the weight estimate"
}

Be conservative with weight estimates. The confidence_percent must be an integer 0-100."""

        resp = requests.post(
            f"{GROQ_BASE_URL}/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": GROQ_VISION_MODEL,
                "messages": [{
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{b64_img}"}}
                    ]
                }],
                "temperature": 0.1,
                "max_tokens": 400
            },
            timeout=60)
        resp.raise_for_status()
        raw = resp.json()["choices"][0]["message"]["content"]

        # Robust JSON extraction
        raw = raw.strip()
        raw = re.sub(r"```(?:json)?\s*", "", raw)
        raw = raw.replace("```", "")
        match = re.search(r"\{[^{}]*\}", raw, re.DOTALL)
        if match:
            raw = match.group(0)
        raw = raw.strip()

        result = json.loads(raw)

        # Validate expected fields
        for field in ["waste_type", "estimated_weight_kg"]:
            if field not in result:
                result[field] = "general_waste" if field == "waste_type" else 0
        if "confidence_percent" not in result:
            result["confidence_percent"] = 50

        try:
            result["estimated_weight_kg"] = float(result["estimated_weight_kg"])
        except (ValueError, TypeError):
            result["estimated_weight_kg"] = 0

        try:
            result["confidence_percent"] = int(result["confidence_percent"])
        except (ValueError, TypeError):
            result["confidence_percent"] = 50
        result["confidence_percent"] = max(0, min(100, result["confidence_percent"]))

        return result
    except json.JSONDecodeError:
        return {"error": f"AI returned invalid JSON. Raw: {raw[:200]}. Try again or enter manually."}
    except requests.exceptions.HTTPError:
        return {"error": f"HTTP Error {resp.status_code}: {resp.text}"}
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


def generate_chat_report(messages: list) -> str:
    """Generate a report strictly from chat history — separate from the Carbon Tax Calculator.
    Parses any quantities mentioned in chat, applies the same EF constants,
    and computes carbon impact using the carbon tax formula."""
    if not messages:
        return "No chat data available to generate a report. Send some messages in the chat first."

    chat_lines = []
    for m in messages:
        sender = m.get("sender", "Unknown")
        text = m.get("text", "")
        ts = m.get("timestamp") or m.get("time", "")
        if text.strip():
            chat_lines.append(f"[{ts}] {sender}: {text}")

    if not chat_lines:
        return "No meaningful chat content found to generate a report."

    chat_text = "\n".join(chat_lines)

    prompt = f"""You are GreenLoom AI - generating a Chat Report strictly from chat conversation data.

CHAT HISTORY (most recent first):
{chat_text}

TASK: Analyse ONLY the chat data above. Do NOT reference any external calculator data.

1. Summarise the key topics discussed.
2. Extract any quantitative data mentioned (fuel litres, kWh, distance km, waste kg, etc.).
3. For each extracted value, calculate CO2e using these emission factors:
   - Petrol: {EF['petrol_litre']} kg CO2e/litre
   - Diesel: {EF['diesel_litre']} kg CO2e/litre
   - LPG: {EF['lpg_kg']} kg CO2e/kg
   - Electricity: {EF['electricity_kwh']} kg CO2e/kWh
   - Logistics distance: {EF['logistics_km']} kg CO2e/km
   - Logistics diesel: {EF['diesel_litre']} kg CO2e/litre
   - Solid waste: {EF['solid_waste_kg']} kg CO2e/kg
   - Wastewater: {EF['wastewater_m3']} kg CO2e/m3
   - Scrap metal: {EF['scrap_metal_kg']} kg CO2e/kg
   - General waste: {EF['general_waste_kg']} kg CO2e/kg
4. Sum all CO2e into total tonnes. Apply the Malaysian carbon tax:
   - Free allowance: {FREE_ALLOWANCE_TONNES:,} tonnes
   - Tax rate: RM {CARBON_TAX_RATE_RM}/tonne CO2e
   - Taxable = max(0, total - {FREE_ALLOWANCE_TONNES:,})
   - Tax payable = taxable x RM {CARBON_TAX_RATE_RM}
5. For logistics consumption, compute:
   - Logistics CO2e = (distance_km x {EF['logistics_km']}) + (diesel_litres x {EF['diesel_litre']})
   - Logistics carbon tax = (logistics CO2e / 1000) x RM {CARBON_TAX_RATE_RM}
6. Provide actionable recommendations.

IMPORTANT: This report is based ONLY on chat conversation data. It is separate from the Carbon Tax Calculator manual entries.

Format the report with clear sections and bullet points."""

    api_payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": "You are GreenLoom AI, a sustainability analyst. Generate a detailed chat report based on the conversation data provided."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.3,
        "max_tokens": 4096
    }

    for attempt in range(3):
        try:
            resp = requests.post(
                f"{CUSTOM_BASE_URL}/chat/completions",
                headers={"Authorization": f"Bearer {Z_AI_API_KEY}", "Content-Type": "application/json"},
                json=api_payload,
                timeout=60)
            resp.raise_for_status()
            data = resp.json()

            choices = data.get("choices", [])
            if not choices:
                if attempt < 2:
                    continue
                return "API returned no choices. Please try again."

            msg = choices[0].get("message", {})
            content = msg.get("content") or ""
            finish = choices[0].get("finish_reason", "")

            if not content.strip():
                if attempt < 2:
                    continue
                return f"AI returned an empty response (finish_reason: {finish}). Please try again."

            if finish == "length":
                content += "\n\n---\n*Report truncated due to length. Ask follow-up questions in chat for more detail.*"

            return content

        except requests.exceptions.HTTPError:
            return f"HTTP Error {resp.status_code}: {resp.text}"
        except requests.exceptions.Timeout:
            if attempt < 2:
                continue
            return "AI request timed out after 3 attempts. Please try again."
        except Exception as e:
            if attempt < 2:
                continue
            return f"AI Error: {str(e)}"


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
def calculate_carbon_tax(scope1, scope2, scope3_logistics, scope3_waste, offset_t):
    scope3 = scope3_logistics + scope3_waste
    total_kg  = scope1 + scope2 + scope3
    total_t   = total_kg / 1000.0
    net_t     = max(0, total_t - offset_t)
    taxable_t = max(0, net_t - FREE_ALLOWANCE_TONNES)
    tax_rm    = taxable_t * CARBON_TAX_RATE_RM
    offset_cost = offset_t * OFFSET_PRICE_RM
    return {
        "scope1_t":  scope1 / 1000,
        "scope2_t":  scope2 / 1000,
        "scope3_t":  scope3 / 1000,
        "scope3_logistics_t": scope3_logistics / 1000,
        "scope3_waste_t":     scope3_waste / 1000,
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
         scope3_logistics_co2e, scope3_waste_co2e, total_co2e, taxable_co2e, carbon_tax_rm,
         offset_co2e, net_tax_rm, supplier_used, notes)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
        st.session_state.user, datetime.now().isoformat(),
        result["scope1_t"], result["scope2_t"],
        result["scope3_t"], result["scope3_logistics_t"],
        result["scope3_waste_t"],
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


def generate_submission_pdf(result: dict, supplier_name: str, notes: str) -> str:
    """Generate a PDF report of the carbon tax breakdown and save to submission_report folder."""
    def _safe(text):
        return text.replace("—", "--").replace("–", "-").replace("‘", "'").replace("’", "'").replace("“", '"').replace("”", '"').replace("…", "...").replace("•", "-").replace("²", "2")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"carbon_tax_report_{ts}.pdf"
    filepath = os.path.join(SUBMISSION_REPORT_FOLDER, filename)

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    # Title
    pdf.set_font("Helvetica", "B", 18)
    pdf.cell(0, 12, "GreenLoom Carbon Tax Report", ln=True, align="C")
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 6, _safe(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"), ln=True, align="C")
    pdf.cell(0, 6, _safe(f"Submitted by: {st.session_state.user} ({st.session_state.role})"), ln=True, align="C")
    pdf.ln(4)
    pdf.set_draw_color(0, 128, 0)
    pdf.set_line_width(0.5)
    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(6)

    # Emissions Breakdown
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, "Emissions Breakdown", ln=True)
    pdf.set_font("Helvetica", "", 10)

    breakdown = [
        ("Scope 1 - Direct Emissions (Fuel)", f"{result['scope1_t']:.3f} t CO2e"),
        ("Scope 2 - Indirect Emissions (Electricity)", f"{result['scope2_t']:.3f} t CO2e"),
        ("Scope 3 - Logistics", f"{result['scope3_logistics_t']:.3f} t CO2e"),
        ("Scope 3 - Waste Generated in Operations", f"{result['scope3_waste_t']:.3f} t CO2e"),
    ]
    for label, val in breakdown:
        pdf.set_font("Helvetica", "", 10)
        pdf.cell(120, 7, _safe(f"  {label}"))
        pdf.set_font("Helvetica", "B", 10)
        pdf.cell(0, 7, _safe(val), ln=True)

    pdf.ln(3)
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(120, 7, "  Total Emissions")
    pdf.cell(0, 7, _safe(f"{result['total_t']:.3f} t CO2e"), ln=True)
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(120, 7, "  Offsets Applied")
    pdf.cell(0, 7, _safe(f"- {result['offset_t']:.3f} t"), ln=True)
    pdf.cell(120, 7, "  Net Emissions")
    pdf.cell(0, 7, _safe(f"{result['net_t']:.3f} t CO2e"), ln=True)
    pdf.ln(6)

    # Carbon Tax Calculation
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, "Carbon Tax Calculation", ln=True)
    pdf.set_font("Helvetica", "", 10)

    tax_rows = [
        ("Free Allowance", f"{result['allowance_t']:,} t"),
        ("Net Emissions after Offsets", f"{result['net_t']:.3f} t"),
        ("Taxable Emissions", f"{result['taxable_t']:.3f} t"),
        ("Tax Rate", f"RM {result['tax_rate']:.0f}/tonne"),
        ("Gross Carbon Tax", f"RM {result['tax_rm']:,.2f}"),
        ("Offset Cost", f"RM {result['offset_cost_rm']:,.2f}"),
        ("NET TAX PAYABLE", f"RM {result['net_tax_rm']:,.2f}"),
    ]
    for label, val in tax_rows:
        pdf.cell(120, 7, _safe(f"  {label}"))
        pdf.set_font("Helvetica", "B", 10)
        pdf.cell(0, 7, _safe(val), ln=True)
        pdf.set_font("Helvetica", "", 10)
    pdf.ln(6)

    # Calculation Steps
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, "Calculation Steps", ln=True)
    pdf.set_font("Helvetica", "", 9)
    steps = [
        "Step 1: Activity x Emission Factor (kg CO2e)",
        _safe(f"  Scope 1: {result['scope1_t']*1000:.2f} | Scope 2: {result['scope2_t']*1000:.2f} | Scope 3 Logistics: {result['scope3_logistics_t']*1000:.2f} | Scope 3 Waste: {result['scope3_waste_t']*1000:.2f}"),
        _safe(f"  Total: {result['total_t']:.4f} tonnes"),
        _safe(f"Step 2: Deduct Offsets: {result['total_t']:.4f} - {result['offset_t']:.4f} = {result['net_t']:.4f} t"),
        _safe(f"Step 3: Free Allowance: max(0, {result['net_t']:.4f} - {result['allowance_t']:,}) = {result['taxable_t']:.4f} t taxable"),
        _safe(f"Step 4: Apply Tax Rate: {result['taxable_t']:.4f} x RM {result['tax_rate']} = RM {result['tax_rm']:,.2f}"),
        _safe(f"Step 5: Deduct Offset Cost: RM {result['tax_rm']:,.2f} - RM {result['offset_cost_rm']:,.2f} = RM {result['net_tax_rm']:,.2f}"),
    ]
    for step in steps:
        pdf.cell(0, 5, _safe(step), ln=True)
    pdf.ln(6)

    # Supplier & Notes
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, "Additional Info", ln=True)
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 7, _safe(f"  Supplier: {supplier_name}"), ln=True)
    pdf.cell(0, 7, _safe(f"  Notes: {notes or '-'}"), ln=True)
    pdf.ln(8)

    # Footer
    pdf.set_font("Helvetica", "I", 8)
    pdf.cell(0, 5, "This report was generated by GreenLoom Carbon Tax Calculator.", ln=True, align="C")
    pdf.cell(0, 5, "Malaysian Carbon Capture, Utilisation and Storage Act 2025.", ln=True, align="C")

    pdf.output(filepath)
    return filepath


def render_carbon_calculator():
    st.title("🌿 Carbon Tax Calculator")
    st.caption("Malaysian Carbon Capture, Utilisation and Storage Act 2025 — RM 15/tonne CO₂e")

    if st.button("← Back to Chat"):
        st.session_state.show_carbon = False
        st.rerun()

    st.divider()

    # ── Session state defaults for widget keys ───────
    for k, v in [("petrol_l_in", 0.0), ("diesel_l_in", 0.0), ("lpg_kg_in", 0.0),
                 ("kwh_in", 0.0), ("log_km_in", 0.0), ("log_diesel_in", 0.0),
                 ("sw", 0.0), ("sm", 0.0), ("ww", 0.0), ("gw", 0.0),
                 ("offset_t", 0.0)]:
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
    with st.expander("🟠 SCOPE 3 — Logistics & Supplier Carbon Decision", expanded=True):
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

        st.info(f"**Scope 3 Logistics Subtotal: {s3_kg/1000:.4f} t CO₂e**  |  "
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
    # SCOPE 3 — Waste Generated in Operations (Category 5)
    # ════════════════════════════════════════
    for wk, wv in [("waste_solid_kg", 0.0), ("waste_scrap_kg", 0.0),
                    ("waste_ww_m3", 0.0), ("waste_general_kg", 0.0)]:
        if wk not in st.session_state:
            st.session_state[wk] = wv

    with st.expander("⚫ SCOPE 3 — Waste Generated in Operations (Category 5)", expanded=True):
        st.caption("GHG Protocol Scope 3 Category 5: Disposal & treatment of waste generated. "
                    "Emission factors: Solid 0.5 | Wastewater 0.42/m³ | Scrap metal 1.46 | General 0.5 kg CO₂e/kg")

        # ── Wastage Identify (Groq Vision AI) ────
        with st.container():
            st.markdown("##### 🔍 Wastage Identify — AI Vision Weight Estimator")
            st.caption("Upload a photo of waste and AI will identify the type & estimate the weight. Powered by Groq Vision.")

            waste_img = st.file_uploader(
                "📸 Upload waste photo",
                type=["jpg", "jpeg", "png", "webp", "gif"],
                key="waste_image_uploader"
            )

            if waste_img:
                st.image(waste_img, caption="Uploaded waste image", use_container_width=True)

            if st.button("🤖 Identify Wastage with AI", key="identify_waste_btn"):
                if waste_img:
                    with st.spinner("Groq AI is analysing the waste image..."):
                        result = identify_wastage_with_ai(waste_img)
                    if "error" not in result:
                        st.session_state["waste_ai_result"] = result

                        waste_type = result.get("waste_type", "general_waste")
                        weight = float(result.get("estimated_weight_kg", 0))
                        confidence = result.get("confidence_percent", 50)
                        description = result.get("description", "—")
                        details = result.get("material_details", "—")
                        label = result.get("waste_type_label", "Unknown")

                        if waste_type == "solid_waste":
                            st.session_state.waste_solid_kg = weight
                        elif waste_type == "scrap_metal":
                            st.session_state.waste_scrap_kg = weight
                        elif waste_type == "wastewater":
                            st.session_state.waste_ww_m3 = weight
                        else:
                            st.session_state.waste_general_kg = weight

                        conf_color = "green" if confidence >= 70 else "orange" if confidence >= 40 else "red"
                        st.success(f"✅ Identified: **{label}** — estimated **{weight:.1f} kg**")
                        st.markdown(f"**Confidence:** :{conf_color}[**{confidence}%**]")
                        st.info(f"**Description:** {description}\n\n**Materials:** {details}")

                        if confidence < 40:
                            st.warning("⚠️ Low confidence — please verify the weight manually.")
                    else:
                        st.error(f"❌ {result['error']}")
                else:
                    st.warning("Please upload a waste image first.")

            if "waste_ai_result" in st.session_state:
                r = st.session_state["waste_ai_result"]
                with st.container():
                    st.markdown("**📋 Last AI Identification:**")
                    st.markdown(f"- **Type:** {r.get('waste_type_label', '—')}")
                    st.markdown(f"- **Estimated Weight:** {r.get('estimated_weight_kg', 0)} kg")
                    conf = r.get('confidence_percent', 50)
                    conf_color = "green" if conf >= 70 else "orange" if conf >= 40 else "red"
                    st.markdown(f"- **Confidence:** :{conf_color}[**{conf}%**]")
                    st.markdown(f"- **Details:** {r.get('material_details', '—')}")

        st.divider()

        # ── Manual waste input ────
        st.markdown("##### 🗑️ Waste Quantities (manually adjust AI-identified values)")
        c1, c2 = st.columns(2)
        with c1:
            solid_kg   = st.number_input("🗑️ Solid waste (kg)",           min_value=0.0,
                            value=st.session_state.waste_solid_kg, step=1.0, key="sw")
            scrap_kg   = st.number_input("🔩 Scrap / off-cut metal (kg)", min_value=0.0,
                            value=st.session_state.waste_scrap_kg, step=1.0, key="sm")
        with c2:
            ww_m3      = st.number_input("💧 Wastewater (m³)",            min_value=0.0,
                            value=st.session_state.waste_ww_m3, step=0.1, key="ww")
            general_kg = st.number_input("📦 General / other waste (kg)", min_value=0.0,
                            value=st.session_state.waste_general_kg, step=1.0, key="gw")

        st.text_area("📝 Other waste (describe — for record keeping)",
            placeholder="e.g. Chemical solvent 20L, Packaging foam 50kg …", key="custom_waste")

        waste_kg = (solid_kg*EF["solid_waste_kg"] + ww_m3*EF["wastewater_m3"] +
                    scrap_kg*EF["scrap_metal_kg"] + general_kg*EF["general_waste_kg"])
        st.info(f"**Scope 3 Waste Subtotal: {waste_kg/1000:.4f} t CO₂e**")

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

        c1, c2, c3 = st.columns(3)
        c1.metric("🔴 Scope 1", f"{r['scope1_t']:.3f} t")
        c2.metric("🟡 Scope 2", f"{r['scope2_t']:.3f} t")
        c3.metric("🟠 Scope 3", f"{r['scope3_t']:.3f} t")
        st.caption(f"↳ Scope 3 breakdown — Logistics: {r['scope3_logistics_t']:.3f} t  |  Waste: {r['scope3_waste_t']:.3f} t")
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
Scope 1: `{r['scope1_t']*1000:.2f}` | Scope 2: `{r['scope2_t']*1000:.2f}` | Scope 3 (Logistics): `{r['scope3_logistics_t']*1000:.2f}` | Scope 3 (Waste): `{r['scope3_waste_t']*1000:.2f}`
→ **Total: {r['total_t']:.4f} tonnes**

**Step 2 — Deduct Offsets:** {r['total_t']:.4f} − {r['offset_t']:.4f} = **{r['net_t']:.4f} t**

**Step 3 — Free Allowance:** max(0, {r['net_t']:.4f} − {r['allowance_t']:,}) = **{r['taxable_t']:.4f} t taxable**

**Step 4 — Apply Tax Rate:** {r['taxable_t']:.4f} × RM {r['tax_rate']} = **RM {r['tax_rm']:,.2f}**

**Step 5 — Deduct Offset Cost:** RM {r['tax_rm']:,.2f} − RM {r['offset_cost_rm']:,.2f} = **RM {r['net_tax_rm']:,.2f}**
""")

        col_s, col_c = st.columns(2)
        with col_s:
            if st.button("💾 Save Submission", use_container_width=True):
                supplier = st.session_state.get("carbon_supplier", "—")
                notes = st.session_state.get("carbon_notes", "")
                save_carbon_submission(r, supplier, notes)
                pdf_path = generate_submission_pdf(r, supplier, notes)
                st.success(f"✅ Saved to database.\n📄 Report: `{os.path.basename(pdf_path)}`")
        with col_c:
            if st.button("🗑️ Clear All Inputs", use_container_width=True):
                for k in ["carbon_result", "carbon_supplier", "carbon_notes",
                          "s1_petrol_l", "s1_diesel_l", "s2_kwh", "s3_km", "s3_log_fuel",
                          "waste_solid_kg", "waste_scrap_kg", "waste_ww_m3", "waste_general_kg",
                          "waste_ai_result"]:
                    st.session_state.pop(k, None)
                st.rerun()

    # ── Submission Report Files ──────────
    st.divider()
    st.subheader("📄 Submission Reports")
    pdf_files = sorted(
        [f for f in os.listdir(SUBMISSION_REPORT_FOLDER) if f.lower().endswith(".pdf")],
        reverse=True
    )
    if pdf_files:
        for pf in pdf_files:
            fp = os.path.join(SUBMISSION_REPORT_FOLDER, pf)
            fc1, fc2, fc3 = st.columns([5, 1, 1])
            with fc1:
                st.markdown(f"📄 **{pf}**")
            with fc2:
                with open(fp, "rb") as f:
                    st.download_button(
                        "📥 Download",
                        data=f.read(),
                        file_name=pf,
                        mime="application/pdf",
                        key=f"dl_{pf}"
                    )
            with fc3:
                if st.button("🗑️", key=f"del_{pf}", help=f"Delete {pf}"):
                    st.session_state[f"confirm_del_{pf}"] = True
                    st.rerun()

            # Confirmation dialog
            if st.session_state.get(f"confirm_del_{pf}", False):
                st.warning(f"⚠️ Are you sure you want to delete **{pf}**? This cannot be undone.")
                bc1, bc2 = st.columns(2)
                with bc1:
                    if st.button("✅ Confirm Delete", key=f"yes_{pf}"):
                        os.remove(fp)
                        st.session_state.pop(f"confirm_del_{pf}", None)
                        st.success(f"Deleted {pf}")
                        st.rerun()
                with bc2:
                    if st.button("❌ Cancel", key=f"no_{pf}"):
                        st.session_state.pop(f"confirm_del_{pf}", None)
                        st.rerun()
    else:
        st.caption("No PDF reports generated yet. Save a submission to create one.")

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

    # Chat Report — CEO only, above Carbon Tax Calculator
    if st.session_state.role == "CEO":
        st.subheader("📊 Chat Report")
        if st.button("Generate Chat Report", use_container_width=True):
            with st.spinner("Analysing chat data..."):
                history = data["messages"][-10:]
                st.session_state.current_report = generate_chat_report(history)
                st.session_state.show_report = True
                st.session_state.show_policy = False
                st.session_state.show_carbon = False
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

# VIEW C — Chat Report (CEO only)
elif st.session_state.show_report and st.session_state.role == "CEO":
    st.title("📊 Chat Report")
    st.caption(f"Based on chat data only — Generated at {datetime.now().strftime('%Y-%m-%d %H:%M')}")
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