FROM python:3.10

# Install requirements
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Get the app
COPY src /src
WORKDIR /

CMD python3 -m src.run