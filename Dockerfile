FROM python:3.11-slim

COPY . ./
RUN python3 -m pip install -r requirements.txt

ENTRYPOINT ["python3", "setup_elg_connector.py"]