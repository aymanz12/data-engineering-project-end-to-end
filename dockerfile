FROM prefecthq/prefect:3-latest

WORKDIR /opt/prefect

# Copy requirements (without Prefect itself)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

