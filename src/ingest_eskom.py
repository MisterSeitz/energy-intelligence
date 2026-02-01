
import os
import glob
import json
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client, Client
import pandas as pd
from pypdf import PdfReader
from datetime import datetime
from openai import OpenAI

# Load environment variables
load_dotenv('.env.local')

# Supabase setup
SUPABASE_URL = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
ALIBABA_API_KEY = os.getenv("ALIBABA_CODING_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Missing Supabase credentials in .env.local")

if not ALIBABA_API_KEY:
    print("WARNING: ALIBABA_CODING_API_KEY not found. AI enrichment will be skipped.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Alibaba Qwen Client
client = OpenAI(
    api_key=ALIBABA_API_KEY,
    base_url="https://coding-intl.dashscope.aliyuncs.com/v1",
)

ESKOM_DIR = Path("Eskom")

def extract_pdf_text(filepath):
    try:
        reader = PdfReader(filepath)
        text = ""
        # Extract first 5 pages for better context
        for i, page in enumerate(reader.pages):
            if i >= 5: 
                break
            text += page.extract_text() + "\n"
        return text.strip()
    except Exception as e:
        print(f"Error reading PDF {filepath}: {e}")
        return ""

def analyze_with_llm(text, filename, table_name="energy"):
    if not ALIBABA_API_KEY:
        return {}

    try:
        # Define schema-specific extraction targets
        extraction_targets = ""
        if table_name == "nuclear_energy":
            extraction_targets = """
            - reactor_units (integer): Number of reactor units mentioned.
            - target_year (integer): Target year for completion or operation.
            - capacity_mw (number): Capacity in Megawatts.
            - status (text): Current project status (e.g., Planned, Operational).
            - licensing_stage (text): Current licensing stage.
            - regulatory_body (text): Name of the regulatory body mentioned.
            """
        else:
            extraction_targets = """
            - capacity_mw (number): Capacity in Megawatts.
            - investment_amount (number): Investment amount mentioned.
            - currency (text): Currency for investment (e.g., ZAR, USD).
            - policy_refs (list[text]): List of policy references mentioned.
            - tender_references (list[text]): List of tender references.
            """

        prompt = f"""
        Analyze the following text from a document named '{filename}'.
        Extract the following information in JSON format:
        - summary: A concise summary (max 200 words).
        - sentiment: The overall sentiment (Positive, Negative, Neutral, Warning, Critical).
        - key_entities: List of key organizations, people, or locations.
        {extraction_targets}

        Text content:
        {text[:12000]} 
        """

        response = client.chat.completions.create(
            model="qwen3-coder-plus",
            messages=[
                {"role": "system", "content": "You are an expert energy sector analyst. Output ONLY JSON."},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"}
        )
        
        content = response.choices[0].message.content
        return json.loads(content)

    except Exception as e:
        print(f"LLM Analysis failed for {filename}: {e}")
        return {}

def process_file(filepath: Path):
    filename = filepath.name
    file_stat = filepath.stat()
    created_at = datetime.fromtimestamp(file_stat.st_mtime).isoformat()
    
    classification = "energy" # Default
    if "koeberg" in filename.lower() or "nuclear" in filename.lower() or "tisf" in filename.lower():
        classification = "nuclear_energy"
    
    print(f"Processing {filename} as {classification}...")

    raw_text = ""
    ai_summary = ""
    
    # 1. Extract Text
    if filepath.suffix.lower() == ".pdf":
        raw_text = extract_pdf_text(filepath)
        if not raw_text:
            ai_summary = f"PDF Document: {filename}"
            
    elif filepath.suffix.lower() in [".csv", ".txt"]:
        try:
            df = pd.read_csv(filepath, nrows=10) 
            raw_text = f"CSV Data Preview:\n{df.to_string()}"
            ai_summary = raw_text
        except Exception as e:
             ai_summary = f"CSV Data: {filename}"

    elif filepath.suffix.lower() in [".xlsx", ".xls"]:
        try:
            df = pd.read_excel(filepath, nrows=10)
            raw_text = f"Excel Data Preview:\n{df.to_string()}"
            ai_summary = raw_text
        except Exception as e:
             ai_summary = f"Excel Data: {filename}"
    
    # 2. Analyze with LLM if we have text
    analysis = {}
    if raw_text and len(raw_text) > 50:
        print(f"   Running AI Analysis on {filename}...")
        analysis = analyze_with_llm(raw_text, filename, classification)
    
    # 3. Merge Analysis
    final_summary = analysis.get("summary", ai_summary)
    if not final_summary: final_summary = ai_summary
    
    # Construct Payload
    payload = {
        "title": filename,
        "url": str(filepath),
        "source_feed": "Eskom Seed Data",
        "published": created_at,
        "category": "Report" if filepath.suffix == ".pdf" else "Data",
        "ai_summary": final_summary,
        "sentiment": analysis.get("sentiment", "Neutral"),
        "key_entities": analysis.get("key_entities", []),
        "created_at": datetime.now().isoformat()
    }

    # Map Structured Fields
    if classification == "nuclear_energy":
        payload["energy_type"] = "Nuclear"
        payload["infrastructure_project"] = "True" if "project" in filename.lower() else "False"
        
        # Mapped fields
        if "reactor_units" in analysis: payload["reactor_units"] = analysis["reactor_units"]
        if "target_year" in analysis: payload["target_year"] = analysis["target_year"]
        if "capacity_mw" in analysis: payload["capacity_mw"] = analysis["capacity_mw"]
        if "status" in analysis: payload["status"] = analysis["status"]
        if "licensing_stage" in analysis: payload["licensing_stage"] = analysis["licensing_stage"]
        if "regulatory_body" in analysis: payload["regulatory_body"] = analysis["regulatory_body"]

    elif classification == "energy":
        payload["energy_type"] = "grid" # Default fallback
        if "loadshedding" in filename.lower(): payload["energy_type"] = "grid"
        elif "solar" in filename.lower(): payload["energy_type"] = "solar"
        elif "wind" in filename.lower(): payload["energy_type"] = "wind"
        
        # Mapped fields
        if "capacity_mw" in analysis: payload["capacity_mw"] = analysis["capacity_mw"]
        if "investment_amount" in analysis: payload["investment_amount"] = analysis["investment_amount"]
        if "currency" in analysis: payload["currency"] = analysis["currency"]
        if "policy_refs" in analysis: payload["policy_refs"] = analysis["policy_refs"]
        if "tender_references" in analysis: payload["tender_references"] = analysis["tender_references"]

    # UPSERT
    try:
        # Check by url
        existing = supabase.schema("ai_intelligence").table(classification).select("id").eq("url", str(filepath)).execute()
        
        if existing.data:
            rec_id = existing.data[0]['id']
            print(f"   Updating {filename} (ID: {rec_id})...")
            # Update
            supabase.schema("ai_intelligence").table(classification).update(payload).eq("id", rec_id).execute()
        else:
            # Insert
            supabase.schema("ai_intelligence").table(classification).insert(payload).execute()
            print(f"   Inserted {filename}")
            
    except Exception as e:
        print(f"Failed to upsert {filename}: {e}")

def main():
    if not ESKOM_DIR.exists():
        print(f"Directory {ESKOM_DIR} does not exist.")
        return

    files = [f for f in ESKOM_DIR.glob("**/*") if f.is_file()]
    print(f"Found {len(files)} files in {ESKOM_DIR}")

    for file in files:
        if file.suffix.lower() not in ['.pdf', '.csv', '.xlsx', '.xls', '.txt']:
            continue
        process_file(file)

if __name__ == "__main__":
    main()
