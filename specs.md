
Populating a RAG (Retrieval-Augmented Generation) layer with the CourtListener bulk data is a serious engineering undertaking due to the sheer volume—the full case law database and its embeddings can exceed several terabytes.

Here is a 5-step technical plan to get your data from CourtListener and into a production-ready RAG layer.

Phase 1: Data Acquisition
CourtListener provides data as full PostgreSQL snapshots (CSVs) rather than daily deltas.   

Download Strategy: Navigate to the Bulk Data Browser.

Prioritize Files: To build a functional legal RAG, you need these four tables in order:

Courts: Metadata for all jurisdictions (essential for filtering).   

Dockets: Case names and docket numbers.   

Opinion Clusters: Groups related opinions (Lead, Dissent, Concurrence).   

Opinions: The actual text of the law (this is the largest file).

Embeddings: CourtListener offers Case Law Embeddings (approx. 2TB) generated using a fine-tuned ModernBERT model. If you don't want to generate your own vectors, sync these from their S3 bucket:
  
aws s3 sync s3://com-courtlistener-storage/embeddings/opinions/ . --no-sign-request

Phase 2: Staging Database (PostgreSQL)
Do not try to process raw CSVs for RAG; you need a relational database first to handle the complex joins between opinions and dockets.

Schema Setup: Use the schema.sql provided by Free Law Project.

Optimized Import: Use the COPY command. To speed this up, drop all indexes and constraints before importing and recreate them after.   

SQL
COPY public.search_opinions (id, plain_text, ...) 
FROM 'opinions.csv' WITH (FORMAT csv, ENCODING utf8, HEADER);
Storage Tip: Ensure you have at least 1-2 TB of NVMe storage for the indexed database.

Phase 3: Text Pre-processing & Chunking
Legal text is uniquely structured (citations, footnotes, headers). Naive chunking will break your RAG.

Cleaning: Use the html_with_citations or plain_text fields. Strip out boilerplate headers and footers that don't add legal value.

Semantic Chunking: Instead of fixed-token windows (e.g., 500 tokens), use Recursive Character Splitting on paragraph markers or legal section headers.

Context Enrichment: To prevent "Document-Level Retrieval Mismatch," prepend every chunk with its case metadata (e.g., "Case: Roe v. Wade | Court: Supreme Court | Date: 1973"). This ensures the LLM knows the source during generation.

Phase 4: Vector Ingestion (The RAG Layer)
You need a vector database (Pinecone, Milvus, or Weaviate) to store the chunks.

Embedding Model: If you didn't download their 2TB ModernBERT embeddings, use a legal-specialized model like nlpaueb/legal-bert-base-uncased or OpenAI’s text-embedding-3-small.

Indexing Metadata: Store the following as metadata in your vector DB for filtering during retrieval:

court_id (e.g., filter only for 9th Circuit).

date_filed (e.g., ignore cases before 2000).

opinion_type (Lead vs. Dissent).

Phase 5: Retrieval Strategy
For legal AI, a "Simple RAG" usually fails. Implement Hybrid Search:

Keyword (BM25): For specific legal terms and citations (e.g., "18 U.S.C. § 1030").

Vector (Semantic): For conceptual queries (e.g., "expectation of privacy in digital data").

Reranking: Use a Cross-Encoder (like BGE-Reranker) to take the top 50 results from your hybrid search and select the 5 most relevant for the LLM.

Suggested Next Step
Would you like me to generate a Python script template for chunking CourtListener opinions while preserving their metadata for a vector database like Pinecone?

let reconsier that lets do sc courts  dockets opinion clusters and opinios only 

Gemini said
Focusing on South Carolina (SC) courts makes this project significantly more manageable. Instead of terabytes, you'll likely be dealing with gigabytes, which can be handled on a standard professional workstation or a modest cloud instance.

Here is the refined plan to build your SC-specific legal RAG layer.

1. Filtered Data Acquisition
You don't need the full global bulk files if you only want South Carolina. Use the CourtListener REST API to target the specific SC jurisdictions.

Target Court IDs: * sc: Supreme Court of South Carolina

scctapp: South Carolina Court of Appeals

scusa: U.S. District Court for the District of South Carolina (Optional, if you want federal cases within SC).

Extraction Method: Use the API Search endpoint with the filter court=sc&court=scctapp.

Tip: If you still prefer the bulk CSVs, you will need to download the full files once, import them into PostgreSQL, and then run a SELECT INTO or CREATE VIEW to isolate rows where court_id is in your SC list.

2. Relational Linkage
Even for a single state, you must maintain the relational integrity to make the RAG useful. Your database schema should link these three entities:

Level	Table	Key Data Points for RAG
Top	Dockets	Case name, Date, Docket Number.
Middle	Opinion Clusters	Precedential status (Published vs. Unpublished), Citations.
Bottom	Opinions	The actual text content, Opinion type (Lead, Concurring, Dissent).
Why this matters: When the LLM retrieves a "Chunk," it needs to know if that chunk came from a Dissenting opinion (which isn't law) or the Lead opinion (which is).

3. SC-Specific Text Processing
South Carolina legal documents have specific formatting quirks. When cleaning the plain_text or html_with_citations from the Opinions table:

Normalize Citations: Ensure South Carolina Reports (S.C.) and South Eastern Reporter (S.E.2d) citations are preserved. They are the "anchors" for legal retrieval.

Chunking Strategy: * Use a Recursive Character Splitter with a chunk size of ~1000 tokens and 10% overlap.

Metadata Injection: For every chunk, prepend a header:
"Source: [Case Name], [Citation]. Court: [SC Supreme Court/Appeals]. Date: [Year]."

4. The RAG Layer (Vector DB)
Since the volume is smaller, you can use high-performance, cost-effective tools:

Embedding Model: Since you are focused on SC state law, ModernBERT is excellent, but OpenAI text-embedding-3-small is often cheaper and easier to manage for a single-state library.

Vector Store: Pinecone (Serverless) or ChromaDB (if running locally) will easily handle the ~100k to 500k chunks expected from SC state appellate history.

5. Development Roadmap
Sync: Pull all OpinionClusters for court_id='sc' via API.

Fetch: For each cluster, pull the associated Opinion text.

Clean: Strip HTML tags and boilerplate (e.g., "IT IS SO ORDERED").

Embed: Batch process the text through your embedding model.

Upsert: Push to your Vector DB with court_type, year, and case_name as metadata filters.

Would you like a Python script that uses the CourtListener API to specifically pull and format the South Carolina Supreme Court opinions for your database?

yes I will be using milvus and modernbert

Gemini said
Building a South Carolina-specific RAG layer with Milvus and ModernBERT is a high-performance choice. Since ModernBERT handles longer contexts better than previous models, you can afford slightly larger chunks, which is great for legal opinions where context is king.

Here is your implementation plan and a Python starter script to bridge the CourtListener API to your Milvus instance.

1. Architectural Strategy
Target: SC Supreme Court (sc) and Court of Appeals (scctapp).

Embedding Model: answerdotai/ModernBERT-base (or the large variant).

Vector DB: Milvus (using L2 or IP distance metric).

Metadata: We will store the docket_id, cluster_id, and opinion_id in Milvus to allow for "metadata filtering" (e.g., "Only search cases from 2023").

2. Implementation Script (Python)
You will need pymilvus, sentence-transformers, and requests.

Python
import requests
from sentence_transformers import SentenceTransformer
from pymilvus import connections, FieldSchema, CollectionSchema, DataType, Collection

# 1. Setup ModernBERT
model = SentenceTransformer('answerdotai/ModernBERT-base')

# 2. Connect to Milvus
connections.connect("default", host="localhost", port="19530")

# 3. Define Schema
fields = [
    FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
    FieldSchema(name="opinion_id", dtype=DataType.INT64),
    FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=65535),
    FieldSchema(name="vector", dtype=DataType.FLOAT_VECTOR, dim=768), # ModernBERT base is 768
    FieldSchema(name="court", dtype=DataType.VARCHAR, max_length=20)
]
schema = CollectionSchema(fields, "South Carolina Legal Opinions")
collection = Collection("sc_legal_rag", schema)

def fetch_sc_opinions(court_code="sc"):
    """
    Fetches opinions from CourtListener API for specific SC courts.
    Note: You'll need an API Key for high volume.
    """
    base_url = f"https://www.courtlistener.com/api/v3/opinions/?court={court_code}"
    headers = {"Authorization": "Token YOUR_API_KEY"}
    
    response = requests.get(base_url, headers=headers).json()
    
    for result in response.get('results', []):
        opinion_text = result.get('plain_text')
        if not opinion_text:
            continue
            
        # Basic Chunking (Simple version for brevity)
        # With ModernBERT, you can do ~1024-2048 tokens safely
        chunks = [opinion_text[i:i+2000] for i in range(0, len(opinion_text), 2000)]
        
        for chunk in chunks:
            vector = model.encode(chunk)
            
            # Insert into Milvus
            collection.insert([
                [result['id']], 
                [chunk], 
                [vector.tolist()], 
                [court_code]
            ])

# Run for SC Supreme Court
fetch_sc_opinions("sc")
# Run for SC Court of Appeals
fetch_sc_opinions("scctapp")
3. Key Optimization Tips
ModernBERT Pooling: By default, SentenceTransformer uses mean pooling. For legal text, you might experiment with "CLS" pooling if you find retrieval relevance is low, though mean pooling is generally robust.

The "Dockets" Join: The Opinions API response doesn't always contain the full Case Name. You should use the cluster URL provided in the opinion response to fetch the Opinion Cluster, which contains the case_name.

Milvus Indexing: Once data is loaded, create an IVF_FLAT or HNSW index in Milvus for sub-millisecond search:

Python
index_params = {
    "metric_type": "L2",
    "index_type": "HNSW",
    "params": {"M": 8, "efConstruction": 64}
}
collection.create_index("vector", index_params)
4. Handling the Bulk Files (Optional)
If you decide the API is too slow for the historical backfill (decades of SC law), you should:

Download the Dockets, Clusters, and Opinions CSV snapshots.

Use a local Postgres instance to JOIN them where court_id = 'sc'.

Export that joined table to a JSONL file.

Run a bulk-ingestion script to embed the JSONL into Milvus.

Would you like me to refine the chunking logic to specifically handle "Legal Citations" so they don't get cut in half between vectors?

yes also update the script to target 1 year at a time, giving a message as each year starts and stops, and include a restart feature so we can start and stop the process in case it takes a long time, let me target years for each run. 

Gemini said
To make this production-ready for South Carolina’s historical data, we need to move from a simple script to a stateful ETL (Extract, Transform, Load) pipeline.

This version uses a local progress.json file to track which years and pages have been completed. If the script crashes or you stop it, simply running it again will resume exactly where it left off.

Prerequisites
Milvus: Ensure your Milvus instance is running.

API Key: Get your key from your CourtListener Profile.

ModernBERT: The script uses answerdotai/ModernBERT-base.

The Resumable SC Legal Loader
Python
import requests
import json
import os
import time
from sentence_transformers import SentenceTransformer
from pymilvus import connections, FieldSchema, CollectionSchema, DataType, Collection

# --- CONFIGURATION ---
API_KEY = "YOUR_COURTLISTENER_API_KEY"
COURTS = ["sc", "scctapp"]
START_YEAR = 2020
END_YEAR = 2024
COLLECTION_NAME = "sc_legal_rag"
PROGRESS_FILE = "sc_crawl_progress.json"

# --- INITIALIZE MODELS & DB ---
model = SentenceTransformer('answerdotai/ModernBERT-base')
connections.connect("default", host="localhost", port="19530")

# Create Collection if it doesn't exist
if COLLECTION_NAME not in [c for c in connections.list_connections()]:
    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
        FieldSchema(name="opinion_id", dtype=DataType.INT64),
        FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=65535),
        FieldSchema(name="vector", dtype=DataType.FLOAT_VECTOR, dim=768),
        FieldSchema(name="court", dtype=DataType.VARCHAR, max_length=20),
        FieldSchema(name="year", dtype=DataType.INT64)
    ]
    schema = CollectionSchema(fields, "SC Court Opinions")
    collection = Collection(COLLECTION_NAME, schema)
else:
    collection = Collection(COLLECTION_NAME)

# --- RESTART/PROGRESS LOGIC ---
def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_progress(year, court, last_page):
    progress = load_progress()
    if str(year) not in progress: progress[str(year)] = {}
    progress[str(year)][court] = last_page
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f)

# --- MAIN ETL PIPELINE ---
def process_sc_data(target_years):
    headers = {"Authorization": f"Token {API_KEY}"}
    progress = load_progress()

    for year in target_years:
        print(f"\n>>> Starting Year: {year}")
        
        for court in COURTS:
            # Check if we have a saved page for this year/court
            current_page = progress.get(str(year), {}).get(court, 1)
            print(f"  > Processing Court: {court} (Starting at page {current_page})")

            while True:
                url = f"https://www.courtlistener.com/api/v3/opinions/?court={court}&date_filed__year={year}&page={current_page}"
                response = requests.get(url, headers=headers)
                
                if response.status_code == 404: # No more pages
                    break
                if response.status_code != 200:
                    print(f"!! Error {response.status_code}. Pausing...")
                    time.sleep(10)
                    continue

                data = response.json()
                results = data.get('results', [])

                if not results:
                    break

                for op in results:
                    text = op.get('plain_text') or op.get('html_with_citations')
                    if not text or len(text) < 100: continue

                    # Chunking for ModernBERT (Large overlaps help legal context)
                    chunks = [text[i:i+3000] for i in range(0, len(text), 2500)]
                    
                    embeddings = model.encode(chunks)
                    
                    # Batch Insert to Milvus
                    collection.insert([
                        [op['id']] * len(chunks),
                        chunks,
                        embeddings.tolist(),
                        [court] * len(chunks),
                        [year] * len(chunks)
                    ])

                print(f"    Completed Page {current_page}")
                save_progress(year, court, current_page)
                current_page += 1
                
                # Respect Rate Limits
                time.sleep(0.5)

        print(f"<<< Finished Year: {year}")

if __name__ == "__main__":
    # You can target specific years here
    target_list = range(START_YEAR, END_YEAR + 1)
    process_sc_data(target_list)
Why this works for your project:
The Year Slicer: By using the API parameter date_filed__year={year}, we ensure we are only pulling manageable slices of data.

Restartability: The sc_crawl_progress.json file acts as a bookmark. If you hit Ctrl+C or lose internet, it knows exactly which page of which year to resume.

ModernBERT Optimization: I’ve set the chunk size to ~3000 characters with a 500-character overlap. ModernBERT thrives on this extra context compared to older BERT models.

Milvus Metadata: We are storing the year and court directly in Milvus. This allows you to perform Filtered Searches later, like:

"Find me cases about eminent domain but only in the SC Supreme Court from 2022."

Suggested Next Step
Would you like me to show you how to write the Hybrid Search query for Milvus that combines the ModernBERT vector search with a metadata filter for specific SC courts?

yes I will be calling this from an angular application ai assistant if that matters

Gemini said
Integrating this into an Angular application changes the architecture significantly. You should not run the heavy embedding and Milvus ingestion logic directly from the frontend or a simple API call, as it will timeout and likely crash the browser's memory.

Instead, your Angular app should act as a Control Plane that triggers a Background Worker (FastAPI or Node.js) to handle the heavy lifting.

1. The Architecture
Angular Frontend: Provides a UI to select years, start/stop the process, and view a progress bar.

Backend (FastAPI/Python): Performs the ModernBERT embedding and Milvus upserts.

State Store: A small SQLite DB or JSON file to track the "bookmark" (year/page/court).

2. The Python Backend (Task Handler)
Update your script to be a "Service" that can be polled for status.

Python
import requests
import json
import os
from fastapi import FastAPI, BackgroundTasks
from sentence_transformers import SentenceTransformer
from pymilvus import connections, Collection

app = FastAPI()

# Configuration (Use Environment Variables in Production)
API_KEY = "YOUR_COURTLISTENER_API_KEY"
PROGRESS_FILE = "task_progress.json"
model = SentenceTransformer('answerdotai/ModernBERT-base')
connections.connect("default", host="localhost", port="19530")
collection = Collection("sc_legal_rag")

# In-memory status for Angular to poll
task_status = {"status": "idle", "current_year": None, "page": 0}

def run_ingestion(years: list):
    global task_status
    task_status["status"] = "running"
    
    for year in years:
        task_status["current_year"] = year
        print(f"--- Starting Year: {year} ---")
        
        for court in ["sc", "scctapp"]:
            page = get_saved_page(year, court)
            
            while task_status["status"] == "running":
                url = f"https://www.courtlistener.com/api/v3/opinions/?court={court}&date_filed__year={year}&page={page}"
                resp = requests.get(url, headers={"Authorization": f"Token {API_KEY}"})
                
                if resp.status_code != 200: break
                
                data = resp.json()
                results = data.get('results', [])
                if not results: break

                # Process results and upsert to Milvus (Logic from previous step)
                # ... 
                
                page += 1
                task_status["page"] = page
                save_progress(year, court, page)
        
        print(f"--- Finished Year: {year} ---")
    
    task_status["status"] = "completed"

@app.post("/start-ingestion")
def start(years: list, background_tasks: BackgroundTasks):
    background_tasks.add_task(run_ingestion, years)
    return {"message": "Ingestion started"}

@app.post("/stop-ingestion")
def stop():
    task_status["status"] = "stopped"
    return {"message": "Stopping after current page finishes"}

@app.get("/status")
def get_status():
    return task_status
3. The Angular Service (legal-data.service.ts)
In your Angular app, create a service to talk to this backend.

TypeScript
import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

@Injectable({ providedIn: 'root' })
export class LegalDataService {
  private baseUrl = 'http://localhost:8000';

  constructor(private http: HttpClient) {}

  startIngestion(years: number[]) {
    return this.http.post(`${this.baseUrl}/start-ingestion`, years);
  }

  stopIngestion() {
    return this.http.post(`${this.baseUrl}/stop-ingestion`, {});
  }

  getStatus() {
    return this.http.get(`${this.baseUrl}/status`);
  }
}
4. Advanced: Citations & Semantic Chunking
To prevent your AI from "hallucinating" or losing context when a citation is cut in half, update your Python chunking logic to use Legal-Aware Splitting:

Instead of text[i:i+2000], use a regex to ensure you don't break mid-citation:

Python
import re

def legal_chunker(text, chunk_size=3000):
    # Split by double newlines or common SC citation patterns (e.g., "S.C. 123")
    # This keeps paragraphs and logical legal arguments together
    paragraphs = re.split(r'\n\n+', text)
    chunks = []
    current_chunk = ""

    for p in paragraphs:
        if len(current_chunk) + len(p) < chunk_size:
            current_chunk += p + "\n\n"
        else:
            chunks.append(current_chunk.strip())
            current_chunk = p + "\n\n"
    
    if current_chunk:
        chunks.append(current_chunk.strip())
    return chunks
Next Steps for your Angular Project:
Poll for Status: Use an interval in your Angular component to call getStatus() every 5 seconds to update a progress bar.

Display Metadata: When the AI answers a question in your Angular chat UI, make sure to return the opinion_id and court from Milvus so you can provide a direct link back to the source on CourtListener.

Would you like me to provide the code for a "Citation-Aware" search function that your Angular app can call to retrieve these cases?

1

Gemini said
Since you're ready to move forward, here is the integrated architecture for your South Carolina Legal AI.

This setup ensures that your Angular frontend can trigger and monitor the ingestion process, while the Python/FastAPI backend handles the heavy lifting of calling the CourtListener API, embedding text with ModernBERT, and storing it in Milvus.

1. The Data Strategy: Yearly "Slices"
By targeting South Carolina (sc and scctapp) one year at a time, we avoid API timeouts and memory overflows. We will use the date_filed__year filter.

2. The Python Backend (FastAPI + Milvus)
This script acts as your worker. It includes the Restart Feature by checking a local progress.json.

Python
from fastapi import FastAPI, BackgroundTasks
from sentence_transformers import SentenceTransformer
from pymilvus import connections, FieldSchema, CollectionSchema, DataType, Collection
import requests
import json
import os

app = FastAPI()
model = SentenceTransformer('answerdotai/ModernBERT-base')

# Milvus Connection
connections.connect("default", host="localhost", port="19530")

# State Management
PROGRESS_FILE = "sc_ingestion_progress.json"
status_tracker = {"is_running": False, "current_year": None, "message": "Idle"}

def get_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f: return json.load(f)
    return {}

def save_progress(year, court, page):
    data = get_progress()
    if str(year) not in data: data[str(year)] = {}
    data[str(year)][court] = page
    with open(PROGRESS_FILE, "w") as f: json.dump(data, f)

def ingest_worker(years: list):
    global status_tracker
    status_tracker["is_running"] = True
    
    for year in years:
        status_tracker["current_year"] = year
        status_tracker["message"] = f"Starting Year {year}"
        
        for court in ["sc", "scctapp"]:
            progress = get_progress()
            start_page = progress.get(str(year), {}).get(court, 1)
            
            page = start_page
            while status_tracker["is_running"]:
                url = f"https://www.courtlistener.com/api/v3/opinions/?court={court}&date_filed__year={year}&page={page}"
                # Add your API Key in headers
                resp = requests.get(url, headers={"Authorization": "Token YOUR_KEY"})
                
                if resp.status_code != 200: break # End of results
                
                results = resp.json().get('results', [])
                if not results: break
                
                # Processing & Milvus Upload Logic
                # (Embed with ModernBERT and collection.insert here)
                
                save_progress(year, court, page)
                page += 1
                
        status_tracker["message"] = f"Finished Year {year}"
    
    status_tracker["is_running"] = False
    status_tracker["message"] = "Task Completed"

@app.post("/ingest")
def start_ingestion(years: list, background_tasks: BackgroundTasks):
    background_tasks.add_task(ingest_worker, years)
    return {"status": "Started"}

@app.get("/status")
def get_status():
    return status_tracker
3. The Angular Service
In your Angular app, use a service to control this flow and provide updates to your Assistant UI.

TypeScript
import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { interval, switchMap } from 'rxjs';

@Injectable({ providedIn: 'root' })
export class IngestionService {
  constructor(private http: HttpClient) {}

  // Trigger the 1-year-at-a-time process
  startYearlyIngestion(years: number[]) {
    return this.http.post('/api/ingest', years);
  }

  // Poll the status for your progress bar/message
  getProgressUpdates() {
    return interval(5000).pipe(
      switchMap(() => this.http.get<any>('/api/status'))
    );
  }
}
4. Search Implementation (RAG Layer)
When your AI Assistant needs to answer a question, use this Hybrid Search logic in your backend to query Milvus:

Vector Search: Use ModernBERT to embed the user's question.

Metadata Filter: If the user asks about a specific time (e.g., "recent cases"), apply a filter in Milvus: expr="year >= 2023".

Output: Return the text chunks along with the opinion_id so your Angular app can link directly to the CourtListener case page.

Next Step
Would you like me to draft the Milvus search function that handles the retrieval part of your RAG, including how to filter by those specific SC courts?

can you give me the front end angular code to allow me to manage the ingestion process

Gemini said
To create an effective management interface for your legal ingestion process, you need an Angular component that handles three things: input for the year range, real-time status polling, and control actions (Start/Stop).

Here is a clean, modular implementation using Angular's HttpClient and Observable patterns.

1. The Ingestion Service
This service handles the communication with your FastAPI backend.

TypeScript
import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, interval } from 'rxjs';
import { switchMap, share } from 'rxjs/operators';

export interface IngestionStatus {
  is_running: boolean;
  current_year: number | null;
  message: string;
  page?: number;
}

@Injectable({ providedIn: 'root' })
export class IngestionService {
  private readonly API_BASE = 'http://localhost:8000'; // Update to your backend URL

  constructor(private http: HttpClient) {}

  startIngestion(years: number[]): Observable<any> {
    return this.http.post(`${this.API_BASE}/ingest`, years);
  }

  stopIngestion(): Observable<any> {
    // This calls the stop endpoint we defined in the FastAPI logic
    return this.http.post(`${this.API_BASE}/stop-ingestion`, {});
  }

  // Polls the backend every 3 seconds for updates
  getStatusUpdates(): Observable<IngestionStatus> {
    return interval(3000).pipe(
      switchMap(() => this.http.get<IngestionStatus>(`${this.API_BASE}/status`)),
      share()
    );
  }
}
2. The Management Component
This component provides the UI logic. It allows you to enter a start and end year, then transforms that into an array for the backend.

TypeScript
import { Component, OnInit, OnDestroy } from '@angular/core';
import { IngestionService, IngestionStatus } from './ingestion.service';
import { Subscription } from 'rxjs';

@Component({
  selector: 'app-ingestion-manager',
  templateUrl: './ingestion-manager.component.html',
  styleUrls: ['./ingestion-manager.component.css']
})
export class IngestionManagerComponent implements OnInit, OnDestroy {
  startYear: number = 2020;
  endYear: number = 2024;
  status: IngestionStatus = { is_running: false, current_year: null, message: 'Idle' };
  private statusSub?: Subscription;

  constructor(private ingestionService: IngestionService) {}

  ngOnInit() {
    // Start polling immediately to check if a process is already running
    this.statusSub = this.ingestionService.getStatusUpdates().subscribe(
      data => this.status = data
    );
  }

  onStart() {
    const years = [];
    for (let i = this.startYear; i <= this.endYear; i++) {
      years.push(i);
    }
    this.ingestionService.startIngestion(years).subscribe();
  }

  onStop() {
    this.ingestionService.stopIngestion().subscribe();
  }

  ngOnDestroy() {
    this.statusSub?.unsubscribe();
  }
}
3. The HTML Template
A simple, functional layout with a progress indicator.

HTML
<div class="ingestion-container">
  <h2>SC Court Data Manager</h2>
  
  <div class="controls" *ngIf="!status.is_running">
    <label>Start Year: </label>
    <input type="number" [(ngModel)]="startYear">
    
    <label>End Year: </label>
    <input type="number" [(ngModel)]="endYear">
    
    <button (click)="onStart()" class="btn-start">Start Ingestion</button>
  </div>

  <div class="status-panel" [class.active]="status.is_running">
    <h3>Current Status: {{ status.message }}</h3>
    
    <div *ngIf="status.is_running" class="spinner-box">
      <p>Now Processing: <strong>{{ status.current_year }}</strong></p>
      <button (click)="onStop()" class="btn-stop">Stop Process</button>
    </div>
  </div>

  <div class="history-log">
    <p>Restart Feature: Active (Progress saved in sc_ingestion_progress.json)</p>
  </div>
</div>
4. Basic CSS
To give it a "Dashboard" feel.

CSS
.ingestion-container {
  padding: 20px;
  border: 1px solid #ddd;
  border-radius: 8px;
  max-width: 500px;
  font-family: sans-serif;
}

.status-panel {
  margin-top: 20px;
  padding: 15px;
  background: #f4f4f4;
  border-radius: 4px;
}

.status-panel.active {
  background: #e3f2fd;
  border-left: 5px solid #2196f3;
}

.btn-start { background: #4caf50; color: white; padding: 10px; border: none; cursor: pointer; }
.btn-stop { background: #f44336; color: white; padding: 10px; border: none; cursor: pointer; }
input { margin: 0 10px; width: 80px; padding: 5px; }
