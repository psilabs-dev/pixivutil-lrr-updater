FROM python:3.12

WORKDIR /workdir
COPY main.py                /workdir/main.py
COPY requirements.txt       /workdir/requirements.txt

RUN pip install -r requirements.txt
ENTRYPOINT [ "python", "main.py" ]
