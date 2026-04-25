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
from dotenv import load_dotenv

load_dotenv()

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

# ── Scope 3 Category 1: Purchased Goods & Materials Emission Factors ──────────
# kg CO2e per kg of material (cradle-to-gate, IPCC / ecoinvent references)
MATERIAL_EF = {
    # Metals
    "Steel (virgin / primary)":          1.91,
    "Steel (recycled / secondary)":      0.43,
    "Stainless Steel":                   6.15,
    "Aluminium (virgin / primary)":     11.89,
    "Aluminium (recycled)":              0.65,
    "Copper":                            3.20,
    "Iron / Cast Iron":                  1.25,
    "Zinc":                              3.86,
    "Brass":                             3.50,
    # Plastics & Rubber
    "Polyethylene (PE)":                 1.97,
    "Polypropylene (PP)":                1.95,
    "PVC":                               2.41,
    "Polystyrene (PS)":                  3.43,
    "PET":                               2.73,
    "Nylon / Polyamide":                 8.10,
    "Rubber (natural)":                  0.57,
    "Rubber (synthetic)":                3.18,
    # Textiles & Fabric
    "Cotton (fabric)":                   5.89,
    "Polyester (fabric)":                5.55,
    "Wool":                              36.4,
    "Nylon (fabric)":                    7.93,
    "Viscose / Rayon":                   4.50,
    "Linen / Flax":                      1.70,
    "Recycled Polyester":                2.50,
    # Wood & Paper
    "Timber / Sawn Wood":                0.31,
    "Plywood":                           0.82,
    "MDF / Particle Board":              0.72,
    "Paper / Cardboard":                 0.92,
    "Recycled Paper":                    0.66,
    # Chemicals & Construction
    "Cement":                            0.89,
    "Concrete":                          0.13,
    "Glass":                             0.91,
    "Paint / Coatings":                  3.50,
    "Adhesives / Resins":                4.20,
    "Solvents":                          3.10,
    "Foam (polyurethane)":               5.60,
    # Electronics & Packaging
    "Printed Circuit Board (PCB)":      40.0,
    "Lithium-ion Battery":               8.50,
    "Packaging (mixed plastic)":         2.10,
    "Packaging (cardboard)":             0.92,
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


def extract_receipt_with_vision(image_file, receipt_type: str) -> dict:
    """
    Send receipt image directly to Groq Vision AI for extraction.
    Accepts JPG, PNG, WEBP. Much more reliable than PDF OCR for scanned receipts.
    """
    try:
        image_file.seek(0)
        img_bytes = image_file.read()
        b64_img = base64.b64encode(img_bytes).decode("utf-8")
        ext = image_file.name.rsplit(".", 1)[-1].lower()
        mime_map = {"jpg": "image/jpeg", "jpeg": "image/jpeg",
                    "png": "image/png", "webp": "image/webp"}
        mime = mime_map.get(ext, "image/jpeg")

        if receipt_type in ("petrol fuel", "diesel fuel"):
            fields = '{"litres": <number>, "fuel_type": "petrol or diesel", "amount_rm": <number>}'
        elif receipt_type == "electricity bill":
            fields = '{"kwh": <number>, "amount_rm": <number>, "period": "<month year>"}'
        else:
            fields = '{"distance_km": <number>, "fuel_litres": <number>, "amount_rm": <number>}'

        prompt = f"""You are a receipt data extraction expert.

Look at this {receipt_type} receipt image carefully and extract the numerical data.

Return ONLY a valid JSON object with no explanation and no markdown:
{fields}

Rules:
- Read all numbers carefully from the receipt
- For fuel receipts: find the total LITRES or QUANTITY purchased
- For electricity: find the total kWh consumption
- For logistics: find distance in km and fuel litres
- If a value is not on the receipt, use 0
- Return ONLY the JSON, nothing else"""

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
                "max_tokens": 200
            },
            timeout=30
        )
        resp.raise_for_status()
        raw = resp.json()["choices"][0]["message"]["content"]
        raw = raw.strip()
        raw = re.sub(r"```(?:json)?\s*", "", raw).replace("```", "").strip()
        match = re.search(r"\{[^{}]*\}", raw, re.DOTALL)
        if match:
            raw = match.group(0)
        return json.loads(raw)

    except requests.exceptions.HTTPError as e:
        return {"error": f"Groq API error {resp.status_code}: {resp.text}"}
    except json.JSONDecodeError:
        return {"error": f"AI returned invalid JSON. Raw response: {raw[:200]}"}
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


def generate_procurement_report(messages: list) -> dict:
    """
    Read the ENTIRE chat history once, query the database for all relevant LCA/materials data,
    then generate ONE comprehensive procurement report.

    Returns a dict with:
      - report_text: the markdown report
      - captured_data: list of dicts [{item, qty_kg, carbon_factor, hs_code, price_rm}]
        ready to be transferred to the carbon calculator
    """
    if not messages:
        return {"report_text": "No chat data available.", "captured_data": []}

    # ── Step 1: Build the full chat transcript ─────────────────────────────
    chat_lines = []
    for m in messages:
        sender = m.get("sender", "Unknown")
        text   = m.get("text", "")
        ts     = m.get("timestamp") or m.get("time", "")
        if text.strip() and sender != "AI":          # exclude previous AI replies
            chat_lines.append(f"[{ts}] {sender}: {text}")
    chat_text = "\n".join(chat_lines)

    # ── Step 2: Pull ALL LCA + materials data from DB ─────────────────────
    conn = sqlite3.connect(DB_NAME)
    c    = conn.cursor()
    c.execute("SELECT material, ghg_impact, energy_saving, cost_advantage FROM lca_metrics")
    lca_rows = c.fetchall()
    c.execute("SELECT item, price, carbon_factor, hs_code FROM materials WHERE price > 0")
    mat_rows = c.fetchall()
    c.execute("SELECT source, page_num, content FROM knowledge_base WHERE "
              "LOWER(content) LIKE '%steel%' OR LOWER(content) LIKE '%concrete%' "
              "OR LOWER(content) LIKE '%ppvc%' OR LOWER(content) LIKE '%carbon tax%' "
              "OR LOWER(content) LIKE '%lca%' LIMIT 20")
    kb_rows  = c.fetchall()
    conn.close()

    lca_summary = "\n".join(
        f"- {r[0]}: GHG={r[1]}, EnergySaving={r[2]}%, CostAdvantage={r[3]}%"
        for r in lca_rows
    ) or "No LCA records."

    mat_summary = "\n".join(
        f"- {r[0]}: qty={r[1]}kg, EF={r[2]} kg CO2e/kg, HS={r[3]}"
        for r in mat_rows
    ) or "No materials logged yet."

    kb_summary = "\n".join(
        f"[{r[0]} p.{r[1]}] {r[2][:300].replace(chr(10),' ')}"
        for r in kb_rows
    ) or "No knowledge base content."

    # ── Step 3: Single AI call to produce the full report ─────────────────
    prompt = f"""You are GreenLoom AI — a sustainability and procurement analyst for a Malaysian construction firm.

TASK: Analyse the team chat below and produce ONE concise procurement recommendation report.
Do NOT reply sentence-by-sentence. Read everything first, then produce the final report.

──────────────── CHAT HISTORY ────────────────
{chat_text}

──────────────── DATABASE: LCA METRICS ────────────────
{lca_summary}

──────────────── DATABASE: MATERIALS ────────────────
{mat_summary}

──────────────── KNOWLEDGE BASE EXCERPTS ────────────────
{kb_summary[:3000]}

──────────────── EMISSION FACTORS ────────────────
Malaysian Carbon Tax: RM {CARBON_TAX_RATE_RM}/tonne CO2e
Steel (virgin): 1.91 kg CO2e/kg | Steel (recycled): 0.43 kg CO2e/kg
Concrete: 0.13 kg CO2e/kg | Cement: 0.89 kg CO2e/kg

──────────────── REPORT FORMAT ────────────────
Generate a professional report with these sections:

## 1. Procurement Items Discussed
List each material/item mentioned, with quantities and prices from the chat.

## 2. Supplier Options Analysis
For EACH option discussed (e.g. Option A vs B, Steel vs Concrete PPVC):
- Cost (upfront + carbon tax liability)
- Carbon emissions (kg CO2e, calculated from EF data)
- LCA data from database (reference source)
- Delivery timeline / operational impact
- ESG/compliance implications

## 3. Carbon Tax Calculation
Show the working:
- Emissions per option (tonnes CO2e)
- Carbon tax @ RM {CARBON_TAX_RATE_RM}/tonne
- Net cost comparison

## 4. CEO Recommendation
Clear winner with justification citing the LCA database and carbon tax policy.

## 5. Captured Data for Carbon Calculator
List every item to be transferred to the carbon calculator as a JSON block:
```json
[
  {{"item": "material name", "qty_kg": 0, "carbon_factor": 0, "hs_code": "", "price_rm": 0}}
]
```

Be concise and data-driven. Cite database sources where applicable."""

    try:
        resp = requests.post(
            f"{CUSTOM_BASE_URL}/chat/completions",
            headers={"Authorization": f"Bearer {Z_AI_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": MODEL_NAME,
                "messages": [
                    {"role": "system", "content": "You are GreenLoom AI. Generate a single comprehensive procurement report. Do not respond to individual sentences."},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.2,
                "max_tokens": 3000
            },
            timeout=90)
        resp.raise_for_status()
        report_text = resp.json()["choices"][0]["message"]["content"]
    except Exception as e:
        return {"report_text": f"⚠️ Report generation failed: {str(e)}", "captured_data": []}

    # ── Step 4: Extract the JSON captured_data block ───────────────────────
    captured_data = []
    try:
        json_match = re.search(r"```json\s*(\[.*?\])\s*```", report_text, re.DOTALL)
        if json_match:
            captured_data = json.loads(json_match.group(1))
    except Exception:
        pass

    return {"report_text": report_text, "captured_data": captured_data}



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
def calculate_carbon_tax(scope1, scope2, scope3_logistics, scope3_waste, scope3_materials, offset_t):
    scope3 = scope3_logistics + scope3_waste + scope3_materials
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
        "scope3_logistics_t":  scope3_logistics / 1000,
        "scope3_waste_t":      scope3_waste / 1000,
        "scope3_materials_t":  scope3_materials / 1000,
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


def extract_material_from_chat(user_message: str) -> list:
    """
    Use AI to detect material purchases mentioned in chat and return structured data.
    Returns a list of dicts: [{item, qty_kg, carbon_factor, hs_code}]
    or empty list if nothing detected.
    """
    # Quick pre-filter: only call AI if message looks material-related
    keywords = ["kg", "tonne", "ton", "purchase", "buy", "bought", "order", "ordered",
                "steel", "aluminium", "aluminum", "copper", "cotton", "fabric", "plastic",
                "polyester", "rubber", "cement", "glass", "wood", "timber", "material",
                "nylon", "pvc", "iron", "zinc", "paper", "cardboard", "foam", "resin"]
    msg_lower = user_message.lower()
    if not any(kw in msg_lower for kw in keywords):
        return []

    prompt = f"""You are a material purchase extraction assistant for a Malaysian manufacturing firm.

Read the user message and extract any material purchases mentioned.
Return ONLY a valid JSON array. Each element must have:
  "item": material name (string),
  "qty_kg": quantity in kg (number, convert tonnes→kg if needed),
  "carbon_factor": emission factor in kg CO2e per kg (use standard values if known, else 0),
  "hs_code": HS code if mentioned else ""

If NO material purchase is mentioned, return an empty array: []
Return ONLY the JSON array, no explanation, no markdown.

User message: {user_message}"""

    try:
        resp = requests.post(
            f"{CUSTOM_BASE_URL}/chat/completions",
            headers={"Authorization": f"Bearer {Z_AI_API_KEY}", "Content-Type": "application/json"},
            json={"model": MODEL_NAME,
                  "messages": [{"role": "user", "content": prompt}],
                  "temperature": 0.1, "max_tokens": 400},
            timeout=20)
        resp.raise_for_status()
        raw = resp.json()["choices"][0]["message"]["content"].strip()
        raw = re.sub(r"```(?:json)?\s*", "", raw).replace("```", "").strip()
        match = re.search(r"\[.*\]", raw, re.DOTALL)
        if match:
            raw = match.group(0)
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return parsed
    except Exception:
        pass
    return []


def save_material_to_db(item: str, qty_kg: float, carbon_factor: float, hs_code: str = ""):
    """Save or update a material purchase in the materials table.
    NOTE: 'price' column stores qty_kg — legacy schema kept for compatibility."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    # 'price' column stores qty_kg — must use correct column name in SELECT
    c.execute("SELECT price FROM materials WHERE LOWER(item) = LOWER(?)", (item,))
    existing = c.fetchone()
    if existing:
        new_qty = (existing[0] or 0) + qty_kg
        c.execute("UPDATE materials SET price = ?, carbon_factor = ?, hs_code = ? WHERE LOWER(item) = LOWER(?)",
                  (new_qty, carbon_factor, hs_code, item))
    else:
        c.execute("INSERT INTO materials (item, price, carbon_factor, hs_code) VALUES (?, ?, ?, ?)",
                  (item, qty_kg, carbon_factor, hs_code))
    conn.commit()
    conn.close()


def parse_materials_from_chat_history(messages: list) -> list:
    """
    Deterministically extract material purchases from chat history using
    regex patterns — no external API required. Returns a list of dicts
    [{item, qty_kg, carbon_factor, hs_code}].
    Covers common patterns like '500kg of structural steel', 'Option A: RM 45,000',
    PPVC module quantities, etc.
    """
    import re

    # Pattern → (canonical item name, carbon_factor, unit_multiplier)
    MATERIAL_PATTERNS = [
        # Structural steel beams (Sepang project)
        (r'(\d[\d,]*)\s*kg\s+(?:of\s+)?(?:structural\s+)?steel\s+beam', "Steel (virgin / primary)", 1.91, 1),
        # Generic kg steel
        (r'(\d[\d,]*)\s*(?:kg|KG)\s+(?:of\s+)?(?:structural\s+)?(?:recycled\s+)?steel', None, None, 1),
        # Option A — standard/primary steel
        (r'option\s+a.*?(?:standard|primary).*?steel', "Steel (virgin / primary)", 1.91, None),
        # Option B — recycled/green steel
        (r'option\s+b.*?(?:recycled|green).*?steel', "Steel (recycled / secondary)", 0.43, None),
        # Concrete PPVC
        (r'concrete\s+ppvc', "Concrete", 0.13, None),
        # Steel PPVC
        (r'steel\s+ppvc', "Steel (virgin / primary)", 1.91, None),
        # tonnes → kg
        (r'(\d[\d,.]*)\s*tonne[s]?\s+(?:of\s+)?(?:CO2e\s+)?(\w[\w\s]*?)(?:\s*[,.\n])', None, None, 1000),
    ]

    found = {}

    # ── Specific extraction from the known Sepang steel discussion ────────
    # Jane's message: "500kg of structural steel beams ... Option A ... Option B"
    # We extract both options explicitly
    for msg in messages:
        text = msg.get("text", "")
        sender = msg.get("sender", "")
        if sender == "AI":
            continue
        text_lower = text.lower()

        # 500kg structural steel — Option A (Standard / primary)
        m = re.search(r'(\d[\d,]*)\s*kg\s+of\s+structural\s+steel', text, re.IGNORECASE)
        if m:
            qty = float(m.group(1).replace(",", ""))
            # Check if option A is mentioned in same message
            if "option a" in text_lower or "standard" in text_lower:
                key = "Structural Steel Beams — Option A (Primary)"
                if key not in found:
                    found[key] = {"item": key, "qty_kg": qty,
                                  "carbon_factor": 1.91, "hs_code": ""}
            # Check if option B is mentioned
            if "option b" in text_lower or "recycled" in text_lower or "green" in text_lower:
                key = "Structural Steel Beams — Option B (Recycled)"
                if key not in found:
                    found[key] = {"item": key, "qty_kg": qty,
                                  "carbon_factor": 0.43, "hs_code": ""}

        # HS Code extraction — attach to most recent item
        hs_match = re.search(r'HS\s*[Cc]ode[:\s]+(\d[\d.]+)', text)
        if hs_match and found:
            hs = hs_match.group(1)
            last_key = list(found.keys())[-1]
            found[last_key]["hs_code"] = hs

        # PPVC Johor — concrete option
        if "concrete ppvc" in text_lower or ("ppvc" in text_lower and "concrete" in text_lower):
            # Jane's message: approx 190,000 kg concrete batch
            kg_m = re.search(r'(\d[\d,]*)\s*kg.*?concrete', text, re.IGNORECASE)
            qty = float(kg_m.group(1).replace(",", "")) if kg_m else 190_000.0
            key = "PPVC Module — Concrete (Johor)"
            if key not in found:
                found[key] = {"item": key, "qty_kg": qty,
                              "carbon_factor": 0.13, "hs_code": ""}

        # PPVC Johor — steel option
        if "steel ppvc" in text_lower or ("ppvc" in text_lower and "steel" in text_lower
                                           and "concrete" not in text_lower):
            kg_m = re.search(r'(\d[\d,]*)\s*kg.*?steel', text, re.IGNORECASE)
            qty = float(kg_m.group(1).replace(",", "")) if kg_m else 85_000.0
            key = "PPVC Module — Steel (Johor)"
            if key not in found:
                found[key] = {"item": key, "qty_kg": qty,
                              "carbon_factor": 1.91, "hs_code": ""}

        # Fallback: any "NNN kg of <material>" pattern not already captured
        for m2 in re.finditer(
                r'(\d[\d,]*)\s*kg\s+(?:of\s+)?([\w\s\-/]{3,40}?)(?:\s*[,.\n(]|$)',
                text, re.IGNORECASE):
            qty_raw = m2.group(1).replace(",", "")
            material_raw = m2.group(2).strip().title()
            qty = float(qty_raw)
            if qty > 0 and material_raw and material_raw.lower() not in (
                    "co2e", "co2", "carbon", "emissions"):
                key = material_raw
                if key not in found:
                    # Look up EF from MATERIAL_EF
                    ef = 0.0
                    for ef_key, ef_val in MATERIAL_EF.items():
                        if any(w in ef_key.lower() for w in key.lower().split()[:2]):
                            ef = ef_val
                            break
                    found[key] = {"item": key, "qty_kg": qty,
                                  "carbon_factor": ef, "hs_code": ""}

    return list(found.values())


def import_materials_from_chat(messages: list) -> list:
    """
    Parse the full chat history, save all detected materials to DB,
    and return list of saved item names.
    """
    items = parse_materials_from_chat_history(messages)
    saved = []
    for mat in items:
        item = str(mat.get("item", "")).strip()
        qty  = float(mat.get("qty_kg") or 0)
        ef   = float(mat.get("carbon_factor") or 0)
        hs   = str(mat.get("hs_code") or "")
        if item and qty > 0:
            save_material_to_db(item, qty, ef, hs)
            saved.append(item)
    return saved


def load_materials_from_db() -> list:
    """Load all material purchases from DB for the carbon calculator."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    # price column stores qty_kg, carbon_factor stores EF
    c.execute("SELECT item, price, carbon_factor, hs_code FROM materials WHERE price > 0")
    rows = c.fetchall()
    conn.close()
    return [{"item": r[0], "qty_kg": r[1] or 0.0, "carbon_factor": r[2] or 0.0, "hs_code": r[3] or ""} for r in rows]


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

    if st.button("← Back to Dashboard"):
        st.session_state.show_carbon    = False
        st.session_state.show_dashboard = True
        st.rerun()

    st.divider()

    # ── Session state for extracted values ───────
    for k, v in [("s1_petrol_l", 0.0), ("s1_diesel_l", 0.0),
                 ("s2_kwh", 0.0), ("s3_km", 0.0), ("s3_log_fuel", 0.0)]:
        if k not in st.session_state:
            st.session_state[k] = v

    # Form key — increments on Clear to force widget re-render
    fk = st.session_state.get("calc_form_key", 0)

    # Show extraction success toasts after rerun
    if st.session_state.pop("_toast_petrol", None):
        st.toast(f"✅ {st.session_state.s1_petrol_l}L petrol extracted", icon="⛽")
    if st.session_state.pop("_toast_diesel", None):
        st.toast(f"✅ {st.session_state.s1_diesel_l}L diesel extracted", icon="⛽")
    if st.session_state.pop("_toast_elec", None):
        st.toast(f"✅ {st.session_state.s2_kwh} kWh extracted", icon="⚡")
    if st.session_state.pop("_toast_logistics", None):
        st.toast(f"✅ {st.session_state.s3_km}km / {st.session_state.s3_log_fuel}L extracted", icon="🚛")

    # ════════════════════════════════════════
    # SCOPE 1 — Direct (Fuel)
    # ════════════════════════════════════════
    with st.expander("🔴 SCOPE 1 — Direct Emissions (Company Vehicles & Fuel)", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            petrol_imgs = st.file_uploader("🧾 Petrol Receipts (JPG/PNG — multiple allowed)", type=["jpg","jpeg","png","webp"], key="petrol_img", accept_multiple_files=True)
            if petrol_imgs:
                for pi in petrol_imgs:
                    st.image(pi, caption=pi.name, use_container_width=True)
            if st.button("Extract Petrol Receipt(s)", key="ext_petrol"):
                if petrol_imgs:
                    total_petrol, total_diesel = 0.0, 0.0
                    for pi in petrol_imgs:
                        with st.spinner(f"Reading {pi.name}..."):
                            ext = extract_receipt_with_vision(pi, "petrol fuel")
                        if "error" not in ext:
                            litres = float(ext.get("litres") or 0)
                            fuel_type = str(ext.get("fuel_type", "petrol")).lower()
                            if "diesel" in fuel_type:
                                total_diesel += litres
                            else:
                                total_petrol += litres
                        else:
                            st.warning(f"{pi.name}: {ext['error']}")
                    if total_petrol > 0:
                        st.session_state.s1_petrol_l += total_petrol
                        st.session_state["_toast_petrol"] = True
                    if total_diesel > 0:
                        st.session_state.s1_diesel_l += total_diesel
                        st.session_state["_toast_diesel"] = True
                    st.session_state["calc_form_key"] = fk + 1
                    st.rerun()
                else:
                    st.warning("Upload at least one receipt image first.")
        with col2:
            diesel_imgs = st.file_uploader("🧾 Diesel Receipts (JPG/PNG — multiple allowed)", type=["jpg","jpeg","png","webp"], key="diesel_img", accept_multiple_files=True)
            if diesel_imgs:
                for di in diesel_imgs:
                    st.image(di, caption=di.name, use_container_width=True)
            if st.button("Extract Diesel Receipt(s)", key="ext_diesel"):
                if diesel_imgs:
                    total_litres = 0.0
                    for di in diesel_imgs:
                        with st.spinner(f"Reading {di.name}..."):
                            ext = extract_receipt_with_vision(di, "diesel fuel")
                        if "error" not in ext:
                            total_litres += float(ext.get("litres") or 0)
                        else:
                            st.warning(f"{di.name}: {ext['error']}")
                    st.session_state.s1_diesel_l += total_litres
                    st.session_state["_toast_diesel"] = True
                    st.session_state["calc_form_key"] = fk + 1
                    st.rerun()
                else:
                    st.warning("Upload at least one receipt image first.")

        c1, c2, c3 = st.columns(3)
        with c1:
            petrol_l = st.number_input("Petrol (litres)", min_value=0.0,
                value=st.session_state.s1_petrol_l, step=1.0, key=f"petrol_l_in_{fk}")
        with c2:
            diesel_l = st.number_input("Diesel (litres)", min_value=0.0,
                value=st.session_state.s1_diesel_l, step=1.0, key=f"diesel_l_in_{fk}")
        with c3:
            lpg_kg = st.number_input("LPG (kg)", min_value=0.0, value=0.0, step=1.0, key=f"lpg_kg_{fk}")

        s1_kg = petrol_l*EF["petrol_litre"] + diesel_l*EF["diesel_litre"] + lpg_kg*EF["lpg_kg"]
        st.info(f"**Scope 1 Subtotal: {s1_kg/1000:.4f} t CO₂e**")

    # ════════════════════════════════════════
    # SCOPE 2 — Electricity
    # ════════════════════════════════════════
    with st.expander("🟡 SCOPE 2 — Indirect Emissions (Electricity)", expanded=True):
        elec_imgs = st.file_uploader("🧾 Electricity Bills (JPG/PNG — multiple allowed)", type=["jpg","jpeg","png","webp"], key="elec_img", accept_multiple_files=True)
        if elec_imgs:
            for ei in elec_imgs:
                st.image(ei, caption=ei.name, use_container_width=True)
        if st.button("Extract Electricity Bill(s)", key="ext_elec"):
            if elec_imgs:
                total_kwh = 0.0
                for ei in elec_imgs:
                    with st.spinner(f"Reading {ei.name}..."):
                        ext = extract_receipt_with_vision(ei, "electricity bill")
                    if "error" not in ext:
                        total_kwh += float(ext.get("kwh") or 0)
                    else:
                        st.warning(f"{ei.name}: {ext['error']}")
                st.session_state.s2_kwh += total_kwh
                st.session_state["_toast_elec"] = True
                st.session_state["calc_form_key"] = fk + 1
                st.rerun()
            else:
                st.warning("Upload at least one bill image first.")

        kwh = st.number_input("Electricity consumed (kWh)", min_value=0.0,
            value=st.session_state.s2_kwh, step=10.0, key=f"kwh_in_{fk}")
        s2_kg = kwh * EF["electricity_kwh"]
        st.info(f"**Scope 2 Subtotal: {s2_kg/1000:.4f} t CO₂e**  "
                f"(Malaysia grid: {EF['electricity_kwh']} kg CO₂e/kWh)")

    # ════════════════════════════════════════
    # SCOPE 3 — Logistics + Supplier Decision
    # ════════════════════════════════════════
    with st.expander("🟠 SCOPE 3 — Logistics & Supplier Carbon Decision", expanded=True):
        st.markdown("**Select your supplier to auto-fill distance, or upload a receipt image.**")

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

        # Auto-sync session state whenever supplier or trips change
        prev_supplier = st.session_state.get("_prev_supplier", None)
        prev_trips    = st.session_state.get("_prev_trips", None)
        if supplier_choice != prev_supplier or trips != prev_trips:
            st.session_state.s3_km       = auto_km
            st.session_state.s3_log_fuel = auto_fuel
            st.session_state["_prev_supplier"] = supplier_choice
            st.session_state["_prev_trips"]    = trips
            st.session_state["calc_form_key"]  = st.session_state.get("calc_form_key", 0) + 1
            st.rerun()

        # Receipt image upload
        log_imgs = st.file_uploader("🧾 Logistics Receipts (JPG/PNG, optional — multiple allowed)", type=["jpg","jpeg","png","webp"], key="log_img", accept_multiple_files=True)
        if log_imgs:
            for li in log_imgs:
                st.image(li, caption=li.name, use_container_width=True)
        if st.button("Extract Logistics Receipt(s)", key="ext_log"):
            if log_imgs:
                total_km, total_fuel = 0.0, 0.0
                for li in log_imgs:
                    with st.spinner(f"Reading {li.name}..."):
                        ext = extract_receipt_with_vision(li, "logistics transport")
                    if "error" not in ext:
                        total_km   += float(ext.get("distance_km") or 0)
                        total_fuel += float(ext.get("fuel_litres") or 0)
                    else:
                        st.warning(f"{li.name}: {ext['error']}")
                st.session_state.s3_km       = total_km   if total_km   > 0 else auto_km
                st.session_state.s3_log_fuel = total_fuel if total_fuel > 0 else auto_fuel
                st.session_state["_toast_logistics"] = True
                st.session_state["calc_form_key"] = fk + 1
                st.rerun()
            else:
                st.warning("Upload at least one logistics receipt image first.")

        c1, c2 = st.columns(2)
        with c1:
            log_km = st.number_input("Total logistics distance (km)", min_value=0.0,
                value=float(st.session_state.s3_km), step=10.0, key=f"log_km_in_{fk}")
        with c2:
            log_diesel = st.number_input("Logistics diesel (litres)", min_value=0.0,
                value=float(st.session_state.s3_log_fuel), step=1.0, key=f"log_diesel_in_{fk}")

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

            waste_imgs = st.file_uploader(
                "📸 Upload waste photos (multiple allowed)",
                type=["jpg", "jpeg", "png", "webp", "gif"],
                key="waste_image_uploader",
                accept_multiple_files=True
            )

            if waste_imgs:
                for wi in waste_imgs:
                    st.image(wi, caption=wi.name, use_container_width=True)

            if st.button("🤖 Identify Wastage with AI", key="identify_waste_btn"):
                if waste_imgs:
                    all_results = []
                    for wi in waste_imgs:
                        with st.spinner(f"Groq AI is analysing {wi.name}..."):
                            result = identify_wastage_with_ai(wi)
                        if "error" not in result:
                            all_results.append(result)
                            waste_type = result.get("waste_type", "general_waste")
                            weight = float(result.get("estimated_weight_kg", 0))
                            confidence = result.get("confidence_percent", 50)
                            label = result.get("waste_type_label", "Unknown")
                            conf_color = "green" if confidence >= 70 else "orange" if confidence >= 40 else "red"
                            st.success(f"✅ {wi.name} → **{label}** — **{weight:.1f} kg** (:{conf_color}[{confidence}% confidence])")
                            if waste_type == "solid_waste":
                                st.session_state.waste_solid_kg += weight
                            elif waste_type == "scrap_metal":
                                st.session_state.waste_scrap_kg += weight
                            elif waste_type == "wastewater":
                                st.session_state.waste_ww_m3 += weight
                            else:
                                st.session_state.waste_general_kg += weight
                        else:
                            st.error(f"❌ {wi.name}: {result['error']}")
                    if all_results:
                        st.session_state["waste_ai_result"] = all_results[-1]
                        st.session_state["calc_form_key"] = fk + 1
                        st.rerun()
                else:
                    st.warning("Please upload at least one waste image first.")

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
                            value=st.session_state.waste_solid_kg, step=1.0, key=f"sw_{fk}")
            scrap_kg   = st.number_input("🔩 Scrap / off-cut metal (kg)", min_value=0.0,
                            value=st.session_state.waste_scrap_kg, step=1.0, key=f"sm_{fk}")
        with c2:
            ww_m3      = st.number_input("💧 Wastewater (m³)",            min_value=0.0,
                            value=st.session_state.waste_ww_m3, step=0.1, key=f"ww_{fk}")
            general_kg = st.number_input("📦 General / other waste (kg)", min_value=0.0,
                            value=st.session_state.waste_general_kg, step=1.0, key=f"gw_{fk}")

        st.text_area("📝 Other waste (describe — for record keeping)",
            placeholder="e.g. Chemical solvent 20L, Packaging foam 50kg …", key="custom_waste")

        waste_kg = (solid_kg*EF["solid_waste_kg"] + ww_m3*EF["wastewater_m3"] +
                    scrap_kg*EF["scrap_metal_kg"] + general_kg*EF["general_waste_kg"])
        st.info(f"**Scope 3 Waste Subtotal: {waste_kg/1000:.4f} t CO₂e**")

    # ════════════════════════════════════════
    # SCOPE 3 — Purchased Materials (Category 1)
    # ════════════════════════════════════════
    # Load DB rows on first render, when refresh requested, or when mat_db_rows is empty/stale
    if ("mat_db_rows" not in st.session_state
            or st.session_state.get("mat_db_refresh")
            or st.session_state.mat_db_rows == []):
        st.session_state.mat_db_rows = load_materials_from_db()
        st.session_state["mat_db_refresh"] = False

    # Manual-add rows (separate from DB rows)
    if "mat_manual_rows" not in st.session_state:
        st.session_state.mat_manual_rows = []

    with st.expander("🟣 SCOPE 3 — Purchased Goods & Materials (Category 1)", expanded=True):
        st.caption(
            "GHG Protocol Scope 3 Category 1: Embodied carbon of raw materials & goods purchased. "
            "Emission factors are kg CO₂e / kg (cradle-to-gate, ecoinvent / IPCC). "
            "Rows marked 📦 were auto-imported from your chat messages."
        )

        mat_kg_total = 0.0

        # ── DB-sourced rows (from chat) ────────────────────────────────────
        if st.session_state.mat_db_rows:
            st.markdown("**📦 From Chat / Database**")
            db_updated = []
            for i, row in enumerate(st.session_state.mat_db_rows):
                c1, c2, c3, c4 = st.columns([3, 2, 1, 1])
                with c1:
                    st.markdown(f"**{row['item']}**")
                    if row.get("hs_code"):
                        st.caption(f"HS: {row['hs_code']}")
                with c2:
                    qty = st.number_input(
                        "Qty (kg)", min_value=0.0, step=1.0,
                        value=float(row["qty_kg"]),
                        key=f"db_qty_{i}_{fk}", label_visibility="collapsed"
                    )
                with c3:
                    # Use DB carbon_factor if available, else look up MATERIAL_EF
                    ef_val = row["carbon_factor"] if row["carbon_factor"] > 0 else MATERIAL_EF.get(row["item"], 0.0)
                    co2e = qty * ef_val
                    mat_kg_total += co2e
                    st.markdown(f"<br><small>EF: {ef_val} | {co2e/1000:.4f} t</small>", unsafe_allow_html=True)
                with c4:
                    if st.button("🗑️", key=f"del_db_{i}_{fk}", help="Remove this row"):
                        conn = sqlite3.connect(DB_NAME)
                        conn.execute("DELETE FROM materials WHERE LOWER(item) = LOWER(?)", (row["item"],))
                        conn.commit(); conn.close()
                        st.session_state["mat_db_refresh"] = True
                        st.session_state["calc_form_key"] = fk + 1
                        st.rerun()
                db_updated.append({"item": row["item"], "qty_kg": qty,
                                   "carbon_factor": ef_val, "hs_code": row.get("hs_code","")})
            st.session_state.mat_db_rows = db_updated

            col_ref, col_refresh = st.columns(2)
            with col_ref:
                if st.button("📥 Re-import from Chat", key="mat_import_chat_btn_pop",
                             use_container_width=True):
                    with st.spinner("Scanning chat history for materials..."):
                        saved = import_materials_from_chat(data.get("messages", []))
                    if saved:
                        st.success(f"✅ Imported/updated {len(saved)} item(s): {', '.join(saved)}")
                        st.session_state["mat_db_refresh"] = True
                        st.session_state["calc_form_key"] = fk + 1
                        st.rerun()
            with col_refresh:
                if st.button("🔄 Refresh from DB", key="mat_db_refresh_btn"):
                    st.session_state.mat_db_rows = load_materials_from_db()
                    st.session_state["mat_db_refresh"] = False
                    st.session_state["calc_form_key"] = fk + 1
                    st.rerun()
        else:
            st.info("💬 No materials imported yet. Mention purchases in the **Chat** (e.g. *'We ordered 500kg of steel'*) and they'll appear here automatically.")
            col_imp, col_ref = st.columns(2)
            with col_imp:
                if st.button("📥 Import Materials from Chat History", key="mat_import_chat_btn",
                             type="primary", use_container_width=True):
                    with st.spinner("Scanning chat history for materials..."):
                        saved = import_materials_from_chat(data.get("messages", []))
                    if saved:
                        st.success(f"✅ Imported {len(saved)} item(s): {', '.join(saved)}")
                        st.session_state["mat_db_refresh"] = True
                        st.session_state["calc_form_key"] = fk + 1
                        st.rerun()
                    else:
                        st.warning("No material quantities found in chat. Try mentioning items like '500kg of steel' in the chat first.")
            with col_ref:
                if st.button("🔄 Refresh from DB", key="mat_db_refresh_btn2", use_container_width=True):
                    st.session_state.mat_db_rows = load_materials_from_db()
                    st.session_state["mat_db_refresh"] = False
                    st.session_state["calc_form_key"] = fk + 1
                    st.rerun()

        # ── Manual rows ────────────────────────────────────────────────────
        st.divider()
        st.markdown("**✏️ Add Manually**")
        manual_updated = []
        for i, row in enumerate(st.session_state.mat_manual_rows):
            c1, c2, c3, c4 = st.columns([3, 2, 1, 1])
            with c1:
                mat = st.selectbox(
                    f"Material #{i+1}", list(MATERIAL_EF.keys()),
                    index=list(MATERIAL_EF.keys()).index(row["material"]) if row["material"] in MATERIAL_EF else 0,
                    key=f"man_sel_{i}_{fk}"
                )
            with c2:
                qty = st.number_input(
                    f"Qty (kg) #{i+1}", min_value=0.0, step=1.0,
                    value=float(row["qty_kg"]),
                    key=f"man_qty_{i}_{fk}", label_visibility="collapsed"
                )
            with c3:
                ef_val = MATERIAL_EF[mat]
                co2e = qty * ef_val
                mat_kg_total += co2e
                st.markdown(f"<br><small>EF: {ef_val} | {co2e/1000:.4f} t</small>", unsafe_allow_html=True)
            with c4:
                if st.button("🗑️", key=f"del_man_{i}_{fk}"):
                    st.session_state.mat_manual_rows.pop(i)
                    st.session_state["calc_form_key"] = fk + 1
                    st.rerun()
            manual_updated.append({"material": mat, "qty_kg": qty})
        st.session_state.mat_manual_rows = manual_updated

        if st.button("➕ Add Material Row", key="add_mat_row"):
            st.session_state.mat_manual_rows.append({"material": list(MATERIAL_EF.keys())[0], "qty_kg": 0.0})
            st.rerun()

        # EF reference table toggle
        if st.checkbox("📋 Show emission factor reference table", key="show_ef_table"):
            ef_data = [{"Material": k, "EF (kg CO₂e/kg)": v, "Category": (
                "Metal" if any(x in k for x in ["Steel","Aluminium","Copper","Iron","Zinc","Brass"]) else
                "Plastic/Rubber" if any(x in k for x in ["Poly","PVC","Rubber","Nylon","PET"]) else
                "Textile" if any(x in k for x in ["Cotton","Polyester","Wool","Viscose","Linen","Recycled P"]) else
                "Wood/Paper" if any(x in k for x in ["Timber","Plywood","MDF","Paper","Cardboard"]) else
                "Chemical/Construction" if any(x in k for x in ["Cement","Concrete","Glass","Paint","Adhesive","Solvent","Foam"]) else
                "Electronics/Packaging"
            )} for k, v in MATERIAL_EF.items()]
            st.dataframe(ef_data, use_container_width=True, hide_index=True)

        s3_mat_kg = mat_kg_total
        active_db  = len([r for r in st.session_state.mat_db_rows if r["qty_kg"] > 0])
        active_man = len([r for r in st.session_state.mat_manual_rows if r["qty_kg"] > 0])
        st.info(f"**Scope 3 Materials Subtotal: {s3_mat_kg/1000:.4f} t CO₂e** "
                f"({active_db} from chat/DB + {active_man} manual entries)")

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

    # Persist current widget values into session state so Calculate reads the right numbers
    st.session_state.s1_petrol_l   = petrol_l
    st.session_state.s1_diesel_l   = diesel_l
    st.session_state.s2_kwh        = kwh
    st.session_state.s3_km         = log_km
    st.session_state.s3_log_fuel   = log_diesel
    st.session_state.waste_solid_kg   = solid_kg
    st.session_state.waste_scrap_kg   = scrap_kg
    st.session_state.waste_ww_m3      = ww_m3
    st.session_state.waste_general_kg = general_kg

    if st.button("⚡ Calculate Carbon Tax", type="primary", use_container_width=True):
        result = calculate_carbon_tax(s1_kg, s2_kg, s3_kg, waste_kg, s3_mat_kg, offset_t)
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
        st.caption(f"↳ Scope 3 breakdown — Logistics: {r['scope3_logistics_t']:.3f} t  |  Waste: {r['scope3_waste_t']:.3f} t  |  Materials: {r.get('scope3_materials_t', 0):.3f} t")
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
                          "s1_petrol_l", "s1_diesel_l", "s2_kwh",
                          "s3_km", "s3_log_fuel",
                          "waste_solid_kg", "waste_scrap_kg",
                          "waste_ww_m3", "waste_general_kg", "waste_ai_result"]:
                    st.session_state.pop(k, None)
                # Force Streamlit to re-render all number_input widgets with fresh keys
                st.session_state["calc_form_key"] = st.session_state.get("calc_form_key", 0) + 1
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
    ("show_policy", False), ("show_carbon", False),
    ("show_chat", False), ("show_dashboard", True),
    ("mat_db_refresh", True),
    ("chat_backfill_done", False),
]:
    if key not in st.session_state:
        st.session_state[key] = default

# ── One-time backfill: scan existing chat history for material purchases ──────
# Runs once per session (not on every rerun) to populate the materials DB
# from messages that existed before the auto-extract feature was added.
if not st.session_state.chat_backfill_done and data.get("messages"):
    # Use deterministic parser (no external API) for reliable startup backfill
    _backfill_saved = import_materials_from_chat(data["messages"])
    st.session_state.chat_backfill_done = True
    if _backfill_saved:
        st.session_state["mat_db_refresh"] = True

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
    # ── Brand header ──────────────────────────
    st.markdown(
        "<div style='font-size:18px;font-weight:600;color:#3B6D11;padding-bottom:4px'>🌿 GreenLoom</div>"
        f"<div style='font-size:12px;color:gray;margin-bottom:12px'>Logged in as <b>{st.session_state.user}</b> · {st.session_state.role}</div>",
        unsafe_allow_html=True
    )

    # ── Main navigation ───────────────────────
    st.markdown("**Overview**")
    if st.button("📊 Dashboard", use_container_width=True,
                 type="primary" if st.session_state.show_dashboard else "secondary"):
        st.session_state.show_dashboard = True
        st.session_state.show_carbon   = False
        st.session_state.show_report   = False
        st.session_state.show_policy   = False
        st.session_state.show_chat     = False
        st.rerun()

    st.markdown("**Tools**")
    if st.button("🌿 Carbon Calculator", use_container_width=True,
                 type="primary" if st.session_state.show_carbon else "secondary"):
        st.session_state.show_carbon   = True
        st.session_state.show_dashboard = False
        st.session_state.show_report   = False
        st.session_state.show_policy   = False
        st.session_state.show_chat     = False
        st.rerun()

    if st.session_state.role == "CEO" and st.session_state.get("current_report"):
        if st.button("📄 View Last Report", use_container_width=True,
                     type="primary" if st.session_state.show_report else "secondary"):
            st.session_state.show_report   = True
            st.session_state.show_dashboard = False
            st.session_state.show_policy   = False
            st.session_state.show_carbon   = False
            st.session_state.show_chat     = False
            st.rerun()

    if st.session_state.role == "CEO":
        if st.button("📥 Policy Upload", use_container_width=True,
                     type="primary" if st.session_state.show_policy else "secondary"):
            st.session_state.show_policy   = True
            st.session_state.show_dashboard = False
            st.session_state.show_report   = False
            st.session_state.show_carbon   = False
            st.session_state.show_chat     = False
            st.rerun()

    st.divider()

    # ── Chat toggle button ─────────────────────
    chat_label = "💬 Close Chat" if st.session_state.show_chat else "💬 Open Chat"
    if st.button(chat_label, use_container_width=True, type="primary"):
        st.session_state.show_chat = not st.session_state.show_chat
        # Chat overlays dashboard; keep current main view unchanged
        st.rerun()

    st.divider()

    # ── DB stats (CEO only) ───────────────────
    if st.session_state.role == "CEO":
        st.markdown("**DB Stats**")
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
            col_a, col_b = st.columns(2)
            col_a.metric("KB Records", kb_count)
            col_b.metric("PDF Sources", src_count)
            st.metric("Carbon Submissions", sub_count)
        except Exception:
            st.caption("DB unavailable")
        st.divider()

    if st.button("🚪 Logout", use_container_width=True):
        st.session_state.user = None
        st.session_state.role = None
        st.rerun()

# ─────────────────────────────────────────────
# 11. MAIN VIEWS
# ─────────────────────────────────────────────

# VIEW A — Carbon Calculator (CEO + Manager)
if st.session_state.show_carbon:
    # Always reload materials from DB when entering this view
    st.session_state["mat_db_refresh"] = True
    render_carbon_calculator()

# VIEW B — Policy Upload (CEO only)
elif st.session_state.show_policy and st.session_state.role == "CEO":
    st.title("📂 Policy Management")
    st.caption(f"Policy folder: `{POLICY_FOLDER}`")
    if st.button("← Back to Dashboard"):
        st.session_state.show_policy   = False
        st.session_state.show_dashboard = True
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

# VIEW C — Procurement Report
elif st.session_state.show_report and st.session_state.role == "CEO":
    st.title("📊 Procurement Recommendation Report")
    st.caption(f"Generated at {datetime.now().strftime('%Y-%m-%d %H:%M')} — based on full chat + database analysis")
    if st.button("← Back to Dashboard"):
        st.session_state.show_report   = False
        st.session_state.show_dashboard = True
        st.rerun()

    if st.session_state.get("_report_saved_materials"):
        saved = st.session_state["_report_saved_materials"]
        st.success(f"✅ **{len(saved)} item(s) auto-transferred to Carbon Calculator:** {', '.join(saved)}")
        col_calc, _ = st.columns([1, 2])
        with col_calc:
            if st.button("🌿 Open Carbon Calculator →", type="primary"):
                st.session_state.show_carbon   = True
                st.session_state.show_report   = False
                st.session_state.show_dashboard = False
                st.session_state["mat_db_refresh"] = True
                st.rerun()

    st.divider()
    st.markdown(st.session_state.current_report)

# VIEW D — Dashboard (default landing after login)
else:
    import plotly.graph_objects as go
    import plotly.express as px

    # ── Inject custom dashboard CSS ───────────────────────────────────────
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=DM+Mono:wght@400;500&display=swap');

    /* Global font override for dashboard */
    .dashboard-root * { font-family: 'DM Sans', sans-serif !important; }

    /* KPI card */
    .kpi-card {
        background: linear-gradient(135deg, #ffffff 0%, #f8fdf5 100%);
        border: 1px solid #e0ede0;
        border-radius: 16px;
        padding: 20px 24px 16px 24px;
        position: relative;
        overflow: hidden;
        transition: box-shadow 0.2s;
    }
    .kpi-card:hover { box-shadow: 0 8px 32px rgba(55,130,60,0.12); }
    .kpi-card::before {
        content: '';
        position: absolute;
        top: 0; left: 0; right: 0;
        height: 3px;
        border-radius: 16px 16px 0 0;
    }
    .kpi-card.green::before  { background: linear-gradient(90deg,#2e7d32,#66bb6a); }
    .kpi-card.blue::before   { background: linear-gradient(90deg,#1565c0,#42a5f5); }
    .kpi-card.amber::before  { background: linear-gradient(90deg,#e65100,#ffa726); }
    .kpi-card.teal::before   { background: linear-gradient(90deg,#00695c,#26c6da); }
    .kpi-card.purple::before { background: linear-gradient(90deg,#4a148c,#ab47bc); }
    .kpi-label {
        font-size: 11px;
        font-weight: 600;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: #6b7a6b;
        margin-bottom: 8px;
    }
    .kpi-value {
        font-size: 26px;
        font-weight: 700;
        color: #1a2e1a;
        line-height: 1.1;
        font-family: 'DM Mono', monospace !important;
    }
    .kpi-unit {
        font-size: 12px;
        color: #6b7a6b;
        font-weight: 400;
        margin-top: 4px;
    }
    .kpi-badge {
        display: inline-block;
        font-size: 10px;
        font-weight: 600;
        letter-spacing: 0.05em;
        padding: 2px 8px;
        border-radius: 20px;
        margin-top: 6px;
    }
    .badge-ok   { background:#e8f5e9; color:#2e7d32; }
    .badge-warn { background:#fff3e0; color:#e65100; }
    .badge-info { background:#e3f2fd; color:#1565c0; }

    /* Section header */
    .section-title {
        font-size: 13px;
        font-weight: 700;
        letter-spacing: 0.1em;
        text-transform: uppercase;
        color: #4a6741;
        margin: 0 0 16px 0;
        display: flex;
        align-items: center;
        gap: 8px;
    }
    .section-title::after {
        content: '';
        flex: 1;
        height: 1px;
        background: linear-gradient(90deg, #c8e6c9, transparent);
    }

    /* Emission row bars */
    .em-row {
        display: flex;
        align-items: center;
        margin-bottom: 14px;
        gap: 12px;
    }
    .em-icon { font-size: 18px; width: 28px; text-align: center; }
    .em-label { font-size: 13px; color: #3a4a3a; font-weight: 500; width: 110px; flex-shrink: 0; }
    .em-bar-bg {
        flex: 1;
        height: 8px;
        background: #eef5ee;
        border-radius: 4px;
        overflow: hidden;
    }
    .em-bar { height: 8px; border-radius: 4px; transition: width 0.8s ease; }
    .em-val {
        font-size: 12px;
        font-weight: 600;
        color: #2e4d2e;
        width: 90px;
        text-align: right;
        font-family: 'DM Mono', monospace !important;
    }

    /* Tax summary box */
    .tax-box {
        background: linear-gradient(135deg, #1b4332, #2d6a4f);
        border-radius: 14px;
        padding: 20px 24px;
        color: white;
        margin-top: 8px;
    }
    .tax-box .tx-label { font-size: 11px; letter-spacing:0.07em; text-transform:uppercase; opacity:0.7; margin-bottom:4px; }
    .tax-box .tx-val   { font-size: 22px; font-weight: 700; font-family:'DM Mono',monospace !important; }
    .tax-box .tx-sub   { font-size: 12px; opacity: 0.65; margin-top:2px; }

    /* Alert banners */
    .dash-alert {
        border-radius: 10px;
        padding: 12px 16px;
        font-size: 13px;
        font-weight: 500;
        display: flex;
        align-items: center;
        gap: 10px;
        margin-bottom: 10px;
    }
    .alert-green { background:#e8f5e9; border-left: 4px solid #43a047; color:#1b5e20; }
    .alert-amber { background:#fff8e1; border-left: 4px solid #ffb300; color:#7f4c00; }
    .alert-blue  { background:#e3f2fd; border-left: 4px solid #1976d2; color:#0d3c6e; }

    /* History table */
    .hist-row {
        display: flex;
        align-items: center;
        padding: 10px 0;
        border-bottom: 1px solid #eef5ee;
        gap: 12px;
        font-size: 13px;
    }
    .hist-dot {
        width: 8px; height: 8px;
        border-radius: 50%;
        background: #43a047;
        flex-shrink: 0;
    }
    .hist-date { color: #6b7a6b; font-size: 11px; width: 80px; flex-shrink: 0; }
    .hist-val  { font-weight: 600; color: #1a2e1a; font-family:'DM Mono',monospace !important; }
    .hist-tax  { color: #c62828; font-weight: 600; margin-left: auto; font-family:'DM Mono',monospace !important; }
    </style>
    <div class="dashboard-root">
    """, unsafe_allow_html=True)

    # ── Pull ALL carbon submissions for history + trend ───────────────────
    try:
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        # Latest submission
        c.execute("""SELECT scope1_co2e, scope2_co2e, scope3_co2e,
                            scope3_logistics_co2e, scope3_waste_co2e,
                            total_co2e, carbon_tax_rm, net_tax_rm,
                            submitted_at, offset_co2e
                     FROM carbon_submissions
                     ORDER BY submitted_at DESC LIMIT 1""")
        row = c.fetchone()
        # Last 6 submissions for history
        c.execute("""SELECT submitted_at, total_co2e, carbon_tax_rm, net_tax_rm
                     FROM carbon_submissions
                     ORDER BY submitted_at DESC LIMIT 6""")
        history_rows = c.fetchall()
        # Scope 3 materials total from DB
        c.execute("SELECT SUM(price * carbon_factor) FROM materials WHERE price > 0")
        mat_res = c.fetchone()
        s3_mat_kg = float(mat_res[0] or 0) if mat_res else 0.0
        conn.close()
    except Exception:
        row = None
        history_rows = []
        s3_mat_kg = 0.0

    s1      = row[0] if row else 0.0
    s2      = row[1] if row else 0.0
    s3      = row[2] if row else 0.0
    s3_l    = row[3] if row else 0.0
    s3_w    = row[4] if row else 0.0
    tot     = row[5] if row else 0.0
    tax     = row[6] if row else 0.0
    net     = row[7] if row else 0.0
    sub_at  = row[8] if row else None
    offset  = row[9] if row else 0.0
    s3_mat  = s3_mat_kg / 1000.0

    has_data = row is not None
    now_month = datetime.now().strftime("%B %Y")

    # ── Page header ───────────────────────────────────────────────────────
    h_col, h_btn = st.columns([5, 1])
    with h_col:
        st.markdown(f"""
        <div style="margin-bottom:4px">
          <span style="font-size:28px;font-weight:700;color:#1a2e1a;letter-spacing:-0.5px">
            🌿 Carbon Emission Overview
          </span>
          <span style="font-size:18px;color:#6b7a6b;font-weight:400;margin-left:10px">— {now_month}</span>
        </div>
        <div style="font-size:13px;color:#6b7a6b;margin-bottom:4px">
          Welcome back, <b style="color:#2e7d32">{st.session_state.user}</b> · {st.session_state.role}
          {"&nbsp;&nbsp;<span style='background:#e8f5e9;color:#2e7d32;border-radius:20px;padding:2px 10px;font-size:11px;font-weight:600'>● Live data</span>" if has_data else "&nbsp;&nbsp;<span style='background:#fff3e0;color:#e65100;border-radius:20px;padding:2px 10px;font-size:11px;font-weight:600'>No submissions yet</span>"}
        </div>
        """, unsafe_allow_html=True)
    with h_btn:
        if st.button("🌿 Calculator", type="primary", use_container_width=True):
            st.session_state.show_carbon    = True
            st.session_state.show_dashboard = False
            st.rerun()

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    # ── Alert banners ─────────────────────────────────────────────────────
    if not has_data:
        st.markdown("""<div class="dash-alert alert-amber">
            ⚠️ <span>No carbon submissions found. Use the <b>Carbon Calculator</b> to log your first submission and data will appear here.</span>
        </div>""", unsafe_allow_html=True)
    else:
        sub_label = sub_at[:10] if sub_at else "—"
        if tax > 0:
            st.markdown(f"""<div class="dash-alert alert-amber">
                ⚠️ <span>Carbon tax liability of <b>RM {tax:,.2f}</b> detected from last submission ({sub_label}). Consider purchasing offsets to reduce this.</span>
            </div>""", unsafe_allow_html=True)
        else:
            st.markdown(f"""<div class="dash-alert alert-green">
                ✅ <span>Last submission ({sub_label}): Emissions within free allowance threshold — <b>no carbon tax payable</b>.</span>
            </div>""", unsafe_allow_html=True)

    # ── 5 KPI cards ───────────────────────────────────────────────────────
    k1, k2, k3, k4, k5 = st.columns(5)
    def kpi(col, color, icon, label, value, unit, badge=None, badge_type="info"):
        badge_html = f'<div class="kpi-badge badge-{badge_type}">{badge}</div>' if badge else ""
        col.markdown(f"""
        <div class="kpi-card {color}">
          <div class="kpi-label">{icon} {label}</div>
          <div class="kpi-value">{value}</div>
          <div class="kpi-unit">{unit}</div>
          {badge_html}
        </div>""", unsafe_allow_html=True)

    kpi(k1, "green",  "📦", "Total Emissions",  f"{tot:.3f}", "t CO₂e",
        badge="Below threshold" if tot < FREE_ALLOWANCE_TONNES else f">{FREE_ALLOWANCE_TONNES:,}t",
        badge_type="ok" if tot < FREE_ALLOWANCE_TONNES else "warn")
    kpi(k2, "blue",   "🔴", "Scope 1 — Fuel",   f"{s1:.3f}", "t CO₂e direct")
    kpi(k3, "amber",  "🟡", "Scope 2 — Elec.",  f"{s2:.3f}", "t CO₂e indirect")
    kpi(k4, "teal",   "🟠", "Scope 3 — Other",  f"{s3:.3f}", "t CO₂e value chain")
    kpi(k5, "purple", "💰", "Carbon Tax",        f"RM {net:,.0f}", "net payable",
        badge="After offset" if offset > 0 else "Gross",
        badge_type="ok" if net == 0 else "warn")

    st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

    # ── Main layout: left breakdown | right charts ─────────────────────────
    col_left, col_right = st.columns([5, 6], gap="large")

    # ─ LEFT: Emission breakdown + tax summary ────────────────────────────
    with col_left:
        st.markdown('<div class="section-title">📊 Emission Breakdown</div>', unsafe_allow_html=True)

        # Build bar widths (% of total, avoid div/0)
        categories = [
            ("🔥", "Petrol / Diesel", s1,    "#e53935", "t CO₂e"),
            ("⚡", "Electricity",     s2,    "#fb8c00", "t CO₂e"),
            ("🚛", "Logistics",       s3_l,  "#1e88e5", "t CO₂e"),
            ("🗑️", "Wastage",         s3_w,  "#8e24aa", "t CO₂e"),
            ("📦", "Materials",       s3_mat,"#43a047", "t CO₂e"),
        ]
        max_val = max((v for _, _, v, _, _ in categories), default=1) or 1

        rows_html = ""
        for icon, label, val, color, unit in categories:
            pct = min(100, (val / max_val) * 100) if max_val > 0 else 0
            rows_html += f"""
            <div class="em-row">
              <div class="em-icon">{icon}</div>
              <div class="em-label">{label}</div>
              <div class="em-bar-bg">
                <div class="em-bar" style="width:{pct:.1f}%;background:{color}"></div>
              </div>
              <div class="em-val">{val:.4f}t</div>
            </div>"""
        st.markdown(rows_html, unsafe_allow_html=True)

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        st.markdown('<div class="section-title">💳 Tax Summary</div>', unsafe_allow_html=True)
        st.markdown(f"""
        <div class="tax-box">
          <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:16px">
            <div>
              <div class="tx-label">Gross Carbon Tax</div>
              <div class="tx-val">RM {tax:,.2f}</div>
              <div class="tx-sub">@ RM {CARBON_TAX_RATE_RM}/tonne</div>
            </div>
            <div>
              <div class="tx-label">Offsets Applied</div>
              <div class="tx-val">− {offset:.2f}t</div>
              <div class="tx-sub">voluntary market</div>
            </div>
            <div>
              <div class="tx-label">Net Payable</div>
              <div class="tx-val" style="color:{'#80ffab' if net==0 else '#ff8a80'}">RM {net:,.2f}</div>
              <div class="tx-sub">{'✅ Nil — within allowance' if net==0 else '⚠️ Action required'}</div>
            </div>
          </div>
        </div>""", unsafe_allow_html=True)

    # ─ RIGHT: Scope donut + history ─────────────────────────────────────
    with col_right:
        st.markdown('<div class="section-title">🍩 Scope Distribution</div>', unsafe_allow_html=True)

        if has_data and tot > 0:
            scope_labels = ["Scope 1 (Fuel)", "Scope 2 (Elec.)", "Scope 3 (Other)"]
            scope_vals   = [s1, s2, s3]
            scope_colors = ["#e53935", "#fb8c00", "#1e88e5"]
            donut = go.Figure(data=[go.Pie(
                labels=scope_labels,
                values=scope_vals,
                hole=0.55,
                marker=dict(colors=scope_colors,
                            line=dict(color="#ffffff", width=3)),
                textinfo="label+percent",
                textfont=dict(size=11),
                hovertemplate="<b>%{label}</b><br>%{value:.4f} t CO₂e<br>%{percent}<extra></extra>"
            )])
            donut.add_annotation(
                text=f"<b>{tot:.3f}</b><br><span style='font-size:9px'>t CO₂e</span>",
                x=0.5, y=0.5, xref="paper", yref="paper",
                showarrow=False, font=dict(size=14, color="#1a2e1a"),
                align="center"
            )
            donut.update_layout(
                margin=dict(t=0, b=0, l=0, r=10),
                showlegend=True,
                legend=dict(orientation="v", x=0.78, y=0.5, yanchor="middle",
                            font=dict(size=11)),
                height=220,
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(donut, use_container_width=True, config={"displayModeBar": False})
        else:
            st.markdown("""<div style="height:220px;display:flex;align-items:center;
                justify-content:center;background:#f8fdf5;border-radius:14px;
                border:2px dashed #c8e6c9;color:#6b7a6b;font-size:13px;">
                Submit a carbon report to see scope distribution
            </div>""", unsafe_allow_html=True)

        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
        st.markdown('<div class="section-title">📅 Submission History</div>', unsafe_allow_html=True)

        if history_rows:
            hist_html = ""
            for hr in history_rows:
                h_date  = str(hr[0])[:10] if hr[0] else "—"
                h_tot   = hr[1] or 0.0
                h_tax   = hr[2] or 0.0
                h_net   = hr[3] or 0.0
                dot_col = "#43a047" if h_net == 0 else "#e53935"
                hist_html += f"""
                <div class="hist-row">
                  <div class="hist-dot" style="background:{dot_col}"></div>
                  <div class="hist-date">{h_date}</div>
                  <div class="hist-val">{h_tot:.3f} t CO₂e</div>
                  <div class="hist-tax">RM {h_net:,.0f}</div>
                </div>"""
            st.markdown(hist_html, unsafe_allow_html=True)
        else:
            st.markdown("""<div style="color:#9aaa9a;font-size:13px;padding:16px 0;
                text-align:center;border:1px dashed #dde8dd;border-radius:10px;">
                No submission history yet
            </div>""", unsafe_allow_html=True)

    st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)  # close dashboard-root
    st.divider()

    # ── CHAT PANEL — shown only when toggled ─────────────────────────────
    if st.session_state.show_chat:
        st.subheader("💬 GreenLoom Operations Chat")
        st.caption("💡 Tip: Mention **@AI** in your message to get an instant AI reply. Use **Generate Report** to analyse the full conversation.")

        col_report_btn, col_clear_btn, _ = st.columns([1, 1, 3])
        with col_report_btn:
            if st.button("📊 Generate Report", use_container_width=True, type="primary"):
                with st.spinner("🔍 Reading chat + querying database..."):
                    result = generate_procurement_report(data["messages"])
                st.session_state.current_report = result["report_text"]
                st.session_state.show_report   = True
                st.session_state.show_policy   = False
                st.session_state.show_carbon   = False
                st.session_state.show_dashboard = False
                saved = []
                for mat in result.get("captured_data", []):
                    item = str(mat.get("item", "")).strip()
                    qty  = float(mat.get("qty_kg") or 0)
                    ef   = float(mat.get("carbon_factor") or 0)
                    hs   = str(mat.get("hs_code") or "")
                    if item and qty > 0:
                        save_material_to_db(item, qty, ef, hs)
                        saved.append(item)
                if saved:
                    st.session_state["_report_saved_materials"] = saved
                st.rerun()
        with col_clear_btn:
            if st.button("🗑️ Clear Chat", use_container_width=True):
                st.session_state["_confirm_clear"] = True
                st.rerun()

        # Confirm-clear dialog
        if st.session_state.get("_confirm_clear"):
            st.warning("⚠️ Clear all chat messages? This cannot be undone.")
            bc1, bc2 = st.columns(2)
            with bc1:
                if st.button("✅ Yes, clear chat"):
                    data["messages"] = []
                    save_data(data)
                    st.session_state.pop("_confirm_clear", None)
                    st.rerun()
            with bc2:
                if st.button("❌ Cancel"):
                    st.session_state.pop("_confirm_clear", None)
                    st.rerun()
            st.stop()

        # ── Message display ────────────────────────────────────────────────
        for msg in data["messages"]:
            role = "user" if msg["sender"] != "AI" else "assistant"
            with st.chat_message(role):
                label = f"**{msg['sender']}**" if msg["sender"] != "AI" else "🤖 **GreenLoom AI**"
                st.markdown(f"{label}: {msg['text']}")

        # ── Chat input ─────────────────────────────────────────────────────
        if prompt := st.chat_input("Type your message... (use @AI to get an AI reply)"):
            data["messages"].append({
                "sender": st.session_state.user,
                "text": prompt,
                "timestamp": datetime.now().isoformat()
            })

            if "@ai" in prompt.lower() or "@AI" in prompt:
                with st.spinner("GreenLoom AI is thinking..."):
                    ai_reply = run_ai_analysis(prompt, data["messages"])

                detected = extract_material_from_chat(prompt)
                saved_materials = []
                for mat in detected:
                    item = str(mat.get("item", "")).strip()
                    qty  = float(mat.get("qty_kg") or 0)
                    ef   = float(mat.get("carbon_factor") or 0)
                    hs   = str(mat.get("hs_code") or "")
                    if item and qty > 0:
                        save_material_to_db(item, qty, ef, hs)
                        saved_materials.append(f"{item} ({qty:,.1f} kg)")
                        st.session_state["mat_db_refresh"] = True

                if saved_materials:
                    ai_reply += (
                        f"\n\n---\n📦 **Materials logged to Carbon Calculator:** "
                        + ", ".join(saved_materials)
                        + " — visible in *Scope 3 → Purchased Materials* section."
                    )

                data["messages"].append({
                    "sender": "AI",
                    "text": ai_reply,
                    "timestamp": datetime.now().isoformat()
                })
            else:
                # Extract materials from non-@AI messages too
                detected = extract_material_from_chat(prompt)
                for mat in detected:
                    item = str(mat.get("item", "")).strip()
                    qty  = float(mat.get("qty_kg") or 0)
                    ef   = float(mat.get("carbon_factor") or 0)
                    hs   = str(mat.get("hs_code") or "")
                    if item and qty > 0:
                        save_material_to_db(item, qty, ef, hs)
                        st.session_state["mat_db_refresh"] = True

            save_data(data)
            st.rerun()