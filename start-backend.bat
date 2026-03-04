@echo off
setlocal

cd /d "%~dp0backend"
if not exist ".venv" (
  py -3.12 -m venv .venv
)

call .venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
