# Crowe Content Cleaner Application

This repository now contains a complete working application to clean Crowe export templates and optionally update Risk content using Risk Details input.

## Architecture

- `frontend/`: Angular app for upload, parameter selection, run, and download.
- `backend/`: FastAPI service that executes Stage 1 and Stage 2 cleaning logic.

## Features

- Detects file type automatically (`risk` or `control`) based on sheet names.
- Stage 1 filtering:
  - Risk tabs by `risk_taxonomy_id` (default `80`)
  - Control tabs by `control_taxonomy_id` (default `66`)
- Stage 2 (Risk only):
  - Reads Risk Details file
  - Normalizes Risk IDs (`FS-...` handling)
  - Updates/adds Risk Definitions
  - Adds/links categories and sub-categories using Id vs Row No rules
- Returns a cleaned `.xlsx` preserving original sheet order.

## Prerequisites

- Windows with Python 3.12+ and Node.js 20+
- Internet access for first-time dependency install

## Run (Windows)

1. Start backend:
   - `start-backend.bat`
2. Start frontend in another terminal:
   - `start-frontend.bat`
3. Open browser:
   - `http://localhost:4200`

## Manual Commands

### Backend

```powershell
cd backend
py -3.12 -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### Frontend

```powershell
cd frontend
npm install
npm start
```

## API

- `GET /api/health`
- `POST /api/clean` (multipart form)
  - `export_file` (required)
  - `risk_details_file` (optional, required if `run_stage2=true` for risk files)
  - `risk_taxonomy_id` (int, default `80`)
  - `control_taxonomy_id` (int, default `66`)
  - `run_stage2` (bool, default `true`)

## Important Review Notes on Original Script

Your source file `clean_library_auto.py` currently appears truncated at line 419 and ends mid-function (`process_file_interactive`). This can cause runtime failures if used directly.

The backend implementation here includes the complete processing flow and is safe to run as a standalone service.
