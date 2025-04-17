from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import clickhouse, flatfile
from config.settings import Settings

app = FastAPI(title="Data Ingestion Tool", version="1.0.0")

# CORS middleware configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],  # Vite's default port
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(clickhouse.router, prefix="/api/clickhouse", tags=["clickhouse"])
app.include_router(flatfile.router, prefix="/api/flatfile", tags=["flatfile"])

@app.get("/")
async def root():
    return {"message": "Data Ingestion Tool API"}
