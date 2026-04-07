from fastapi import FastAPI, Depends, HTTPException, status
from google.cloud import bigquery
from pydantic import BaseModel

app = FastAPI()

PROJECT_ID = "proven-agility-477721-q9"
DATASET = "property_mgmt"


# ---------------------------------------------------------------------------
# Dependency: BigQuery client
# ---------------------------------------------------------------------------

def get_bq_client():
    client = bigquery.Client()
    try:
        yield client
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class IncomeCreate(BaseModel):
    income_id: int
    amount: float
    date: str
    description: str


class ExpenseCreate(BaseModel):
    expense_id: int
    amount: float
    date: str
    category: str
    vendor: str
    description: str


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

@app.get("/properties")
def get_properties(bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        ORDER BY property_id
    """

    try:
        results = bq.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    return [dict(row) for row in results]


@app.get("/properties/{property_id}")
def get_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = {property_id}
    """

    rows = [dict(row) for row in bq.query(query).result()]

    if not rows:
        raise HTTPException(status_code=404, detail="Property not found")

    return rows[0]


@app.get("/income/{property_id}")
def get_income(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE property_id = {property_id}
        ORDER BY date
    """

    return [dict(row) for row in bq.query(query).result()]


@app.post("/income/{property_id}")
def create_income(
    property_id: int,
    income: IncomeCreate,
    bq: bigquery.Client = Depends(get_bq_client)
):
    rows_to_insert = [
        {
            "income_id": income.income_id,
            "property_id": property_id,
            "amount": income.amount,
            "date": income.date,
            "description": income.description,
        }
    ]

    table_id = f"{PROJECT_ID}.{DATASET}.income"
    errors = bq.insert_rows_json(table_id, rows_to_insert)

    if errors:
        raise HTTPException(
            status_code=500,
            detail={"message": "Failed to insert income", "errors": errors}
        )

    return {"message": "Income record created successfully"}


@app.get("/expenses/{property_id}")
def get_expenses(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.expenses`
        WHERE property_id = {property_id}
        ORDER BY date
    """

    return [dict(row) for row in bq.query(query).result()]


@app.post("/expenses/{property_id}")
def create_expense(
    property_id: int,
    expense: ExpenseCreate,
    bq: bigquery.Client = Depends(get_bq_client)
):
    rows_to_insert = [
        {
            "expense_id": expense.expense_id,
            "property_id": property_id,
            "amount": expense.amount,
            "date": expense.date,
            "category": expense.category,
            "vendor": expense.vendor,
            "description": expense.description,
        }
    ]

    table_id = f"{PROJECT_ID}.{DATASET}.expenses"
    errors = bq.insert_rows_json(table_id, rows_to_insert)

    if errors:
        raise HTTPException(
            status_code=500,
            detail={"message": "Failed to insert expense", "errors": errors}
        )

    return {"message": "Expense record created successfully"}


@app.get("/properties/{property_id}/summary")
def get_property_summary(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT
            p.property_id,
            p.name,
            p.city,
            p.state,
            p.monthly_rent,
            COUNT(DISTINCT i.income_id) AS income_records,
            COUNT(DISTINCT e.expense_id) AS expense_records
        FROM `{PROJECT_ID}.{DATASET}.properties` p
        LEFT JOIN `{PROJECT_ID}.{DATASET}.income` i
            ON p.property_id = i.property_id
        LEFT JOIN `{PROJECT_ID}.{DATASET}.expenses` e
            ON p.property_id = e.property_id
        WHERE p.property_id = {property_id}
        GROUP BY p.property_id, p.name, p.city, p.state, p.monthly_rent
    """

    results = [dict(row) for row in bq.query(query).result()]
    if not results:
        raise HTTPException(status_code=404, detail="Property not found")
    return results[0]


@app.get("/properties/{property_id}/total-income")
def get_total_income(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT
            property_id,
            SUM(amount) AS total_income
        FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE property_id = {property_id}
        GROUP BY property_id
    """

    results = [dict(row) for row in bq.query(query).result()]
    if not results:
        raise HTTPException(status_code=404, detail="No income found for this property")
    return results[0]


@app.get("/properties/{property_id}/total-expenses")
def get_total_expenses(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT
            property_id,
            SUM(amount) AS total_expenses
        FROM `{PROJECT_ID}.{DATASET}.expenses`
        WHERE property_id = {property_id}
        GROUP BY property_id
    """

    results = [dict(row) for row in bq.query(query).result()]
    if not results:
        raise HTTPException(status_code=404, detail="No expenses found for this property")
    return results[0]


@app.get("/properties/{property_id}/profit")
def get_profit(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        WITH income_totals AS (
            SELECT property_id, SUM(amount) AS total_income
            FROM `{PROJECT_ID}.{DATASET}.income`
            WHERE property_id = {property_id}
            GROUP BY property_id
        ),
        expense_totals AS (
            SELECT property_id, SUM(amount) AS total_expenses
            FROM `{PROJECT_ID}.{DATASET}.expenses`
            WHERE property_id = {property_id}
            GROUP BY property_id
        )
        SELECT
            p.property_id,
            p.name,
            COALESCE(i.total_income, 0) AS total_income,
            COALESCE(e.total_expenses, 0) AS total_expenses,
            COALESCE(i.total_income, 0) - COALESCE(e.total_expenses, 0) AS profit
        FROM `{PROJECT_ID}.{DATASET}.properties` p
        LEFT JOIN income_totals i ON p.property_id = i.property_id
        LEFT JOIN expense_totals e ON p.property_id = e.property_id
        WHERE p.property_id = {property_id}
    """

    results = [dict(row) for row in bq.query(query).result()]
    if not results:
        raise HTTPException(status_code=404, detail="Property not found")
    return results[0]