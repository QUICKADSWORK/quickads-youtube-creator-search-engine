# YouTube Channel Scraper - Dockerfile
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create volume for database persistence
VOLUME /app/data

# Expose port
EXPOSE 8000

# Run the application
CMD ["python", "main.py"]
