# One image for every Python service in the stack. Which one runs is decided by
# the compose command, because they share dependencies and differ only in entry
# point.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

RUN adduser --system --group --no-create-home sentinel

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY collector/ ./collector/
COPY sink/ ./sink/
COPY dashboard/ ./dashboard/

USER sentinel

CMD ["python", "-m", "sink"]
