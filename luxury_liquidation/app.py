"""
Lumina Backend Engine (FastAPI)
Provides the data-capture engine for the Lumina Concierge Liquidation landing page.
Handles form submissions, stores leads in a local SQLite database, and serves the static frontend.
"""

from fastapi import FastAPI, Request, Form, status
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import sqlite3
import os
import logging
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from google import genai

# Load environment variables from .env file
load_dotenv()

# Initialize Gemini Client
client = genai.Client()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Initialize FastAPI application
app = FastAPI(title="Lumina Concierge Liquidation Backend", version="1.0.0")

# Define paths
BASE_DIR = Path(__file__).resolve().parent
DB_FILE = BASE_DIR / "valuations.db"
ASSETS_DIR = BASE_DIR / "assets"
INDEX_HTML = BASE_DIR / "index.html"

# Mount static directory to serve assets (images, videos, etc.) if it exists
if ASSETS_DIR.exists():
    app.mount("/assets", StaticFiles(directory=str(ASSETS_DIR)), name="assets")

def init_db():
    """
    Initializes the SQLite database.
    Creates the 'leads' table if it doesn't exist to store captured form data.
    """
    conn = None
    try:
        # Establish a connection to the SQLite database
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Create the leads table securely
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                zip_code TEXT NOT NULL,
                equipment TEXT NOT NULL,
                condition TEXT NOT NULL,
                timeline TEXT NOT NULL,
                estimated_value TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Safely add the column if the table already existed prior to Phase 2
        try:
            cursor.execute("ALTER TABLE leads ADD COLUMN estimated_value TEXT")
        except sqlite3.OperationalError:
            pass # Column already exists
        
        # Commit the transaction
        conn.commit()
        logger.info("Database initialized successfully.")
    except sqlite3.Error as e:
        logger.error(f"Database initialization error: {e}")
    finally:
        # Close connection and release resources
        if conn:
            conn.close()

# Initialize the database on startup
init_db()

@app.get("/", response_class=HTMLResponse)
async def serve_index():
    """
    Root route: Serves the index.html landing page.
    """
    if INDEX_HTML.exists():
        return FileResponse(str(INDEX_HTML))
    else:
        logger.error(f"Cannot find {INDEX_HTML}")
        return HTMLResponse(content="<h1>Index Not Found</h1>", status_code=status.HTTP_404_NOT_FOUND)

async def generate_preliminary_valuation(equipment_details: str, condition: str):
    """
    Calls the Gemini API to get a concise estimated cash-buyout range.
    """
    try:
        prompt = (
            "System: You are the Head Appraiser for Lumina, a luxury wholesale liquidator. "
            "Evaluate the following equipment details and condition. "
            "Break down the valuation item by item. For each distinct piece of equipment, provide a single bullet point "
            "with its specific estimated cash-buyout range (roughly 40-60% of the used retail market value). "
            "At the bottom, provide a Total Cash Offer range. Keep descriptions extremely brief. Do not quote retail MSRP.\n\n"
            f"Equipment: {equipment_details}\nCondition: {condition}"
        )
        response = await client.aio.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt
        )
        return response.text.strip()
    except Exception as e:
        logger.error(f"Gemini API Error: {e}")
        return 'High-Value Asset Detected: Pending manual expert review.'

def write_to_obsidian_vault(lead_data):
    """
    Export lead data to the Obsidian Vault directory as a Markdown file.
    """
    # Create leads_export directory inside BASE_DIR
    export_dir = BASE_DIR / "leads_export"
    export_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = "".join(c for c in lead_data.get('name', 'Unknown') if c.isalnum() or c in " _-").replace(" ", "_")
    filename = f"Lead_{safe_name}_{timestamp}.md"
    file_path = export_dir / filename
    
    markdown_content = f"""---
title: "Lead: {lead_data.get('name', 'Unknown')}"
tags: [lead, valuation]
---
# New Lead: {lead_data.get('name', 'Unknown')}
**Zip Code:** {lead_data.get('zip_code', 'Unknown')}
**Equipment:** {lead_data.get('equipment', 'Unknown')}
**Condition:** {lead_data.get('condition', 'Unknown')}
**Preferred Timeline:** {lead_data.get('timeline', 'Unknown')}
**Estimated Value:** {lead_data.get('estimated_value', 'Unknown')}
"""
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(markdown_content)
        logger.info(f"Lead securely exported to vault: {file_path}")
    except Exception as e:
        logger.error(f"Failed to export lead to vault: {e}")

@app.post("/submit-valuation")
async def submit_valuation(
    name: str = Form(...),
    zip_code: str = Form(...),
    equipment: str = Form(...),
    condition: str = Form(...),
    timeline: str = Form(...)
):
    """
    POST Endpoint: Catches the valuation form submission and securely stores the lead in the SQLite database.
    Requires Name, Zip Code, Equipment Make/Model, Condition, and Preferred Timeline.
    """
    # 1. Generate preliminary AI valuation
    estimated_value = await generate_preliminary_valuation(equipment, condition)
    
    # 2. Trigger Obsidian alert stub
    lead_data = {
        "name": name,
        "zip_code": zip_code,
        "equipment": equipment,
        "condition": condition,
        "timeline": timeline,
        "estimated_value": estimated_value
    }
    write_to_obsidian_vault(lead_data)

    conn = None
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Securely insert the parsed form data into the leads table using parameterized queries
        # Parameterized queries prevent SQL injection attacks
        cursor.execute('''
            INSERT INTO leads (name, zip_code, equipment, condition, timeline, estimated_value)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (name, zip_code, equipment, condition, timeline, estimated_value))
        
        # Commit transaction
        conn.commit()
        
        logger.info(f"New lead captured from {name} in {zip_code}")
        
        # Return success response
        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content={"status": "success", "message": "Valuation request submitted successfully."}
        )
        
    except sqlite3.Error as e:
        # Handle database errors gracefully
        logger.error(f"Database error during lead insertion: {e}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"status": "error", "message": "An error occurred while saving your request."}
        )
    finally:
        # Ensure the database connection is closed
        if conn:
            conn.close()

if __name__ == "__main__":
    import uvicorn
    # Make sure to run the application using uvicorn in production.
    # e.g., uvicorn app:app --host 0.0.0.0 --port 8080
    uvicorn.run("app:app", host="0.0.0.0", port=8080, reload=True)
