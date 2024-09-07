FROM 192.168.13.252:5050/python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY /pip.conf /etc/pip.conf

RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade pip

COPY requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

RUN apt-get autoremove --purge -y git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY . /app/

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]