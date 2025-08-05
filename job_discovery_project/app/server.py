from fastapi import FastAPI

from ..search.search_index import search_jobs

app = FastAPI()


@app.get("/search")
def search(q: str):
    """Return job search results for the given query."""
    results = search_jobs(q)
    return [job.__dict__ for job in results]
