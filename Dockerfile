# Use slim variant to keep image small
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy and install dependencies first → better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Make sure config.json exists (you'll mount your real one)
# You can also COPY config.json . but usually better to mount it

# Run the application
CMD ["python", "app.py"]