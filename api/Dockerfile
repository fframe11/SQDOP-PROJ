FROM python:3.10-slim

# Create a non‑root user with a dedicated home directory
RUN groupadd -r appgroup && useradd -r -g appgroup -d /home/appuser -m appuser

WORKDIR /app

COPY requirements.txt .
COPY wheels ./wheels
RUN pip install --no-cache-dir --no-index --find-links=wheels -r requirements.txt
RUN pip install --no-cache-dir pandas pyarrow

COPY . .

# Change ownership of the working directory to the non‑root user
RUN chown -R appuser:appgroup /app

# Switch to the non‑root user
USER appuser

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
