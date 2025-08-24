from fastapi import FastAPI
from api.routers import listings

app = FastAPI(
    title="Real Estate Listings API",
    version="1.0.0",
    description="Query the current snapshot (SCD2) of real-estate listings."
)

app.include_router(listings.router)

@app.get("/api/health")
def health():
    return {"status": "ok"}
