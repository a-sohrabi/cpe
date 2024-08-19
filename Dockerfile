FROM 192.168.13.252:5050/python:3.12-slim

WORKDIR /app

COPY /pip.conf /etc/pip.conf

RUN apt update

RUN apt install -y git

RUN pip install --upgrade pip

COPY requirements.txt /app/

RUN pip install -r  requirements.txt

RUN apt autoremove --purge git -y

COPY . /app/

EXPOSE 8001

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]