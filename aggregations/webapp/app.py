from pathlib import Path

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from aggregations.repos.invoices import InvoicesRepo
from aggregations.repos.aggregates import AggregatesRepo

templates = Jinja2Templates(directory=Path(__file__).parent / "templates")

app = FastAPI()


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/invoices")
async def create_invoice(
    request: Request, name: str = Form(...), total: int = Form(...)
):
    InvoicesRepo.create(name, total)
    rows = InvoicesRepo.get_all()
    return templates.TemplateResponse(
        "invoices.html", {"request": request, "rows": rows}
    )


@app.put("/invoices/{id}")
async def update_invoice(
    request: Request, id: int, name: str = Form(...), total: int = Form(...)
):
    InvoicesRepo.update(id, name, total)
    rows = InvoicesRepo.get_all()
    return templates.TemplateResponse(
        "invoices.html", {"request": request, "rows": rows}
    )


@app.delete("/invoices/{id}")
async def delete_invoice(request: Request, id: int):
    InvoicesRepo.delete(id)
    rows = InvoicesRepo.get_all()
    return templates.TemplateResponse(
        "invoices.html", {"request": request, "rows": rows}
    )


@app.get("/invoices", response_class=HTMLResponse)
async def list_invoices(request: Request):
    rows = InvoicesRepo.get_all()
    return templates.TemplateResponse(
        "invoices.html", {"request": request, "rows": rows}
    )


@app.get("/aggregations")
async def aggregations(request: Request):
    total = AggregatesRepo.get_total()
    return templates.TemplateResponse(
        "aggregate.html", {"request": request, "total": total}
    )
