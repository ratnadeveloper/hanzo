FROM python:3.10-slim-bookworm

# Update package lists and install required packages + Node.js
RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y git curl wget bash ffmpeg && \
    curl -fsSL https://deb.nodesource.com/setup_18.x | bash - && \
    apt-get install -y nodejs && \
    rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip3 install --upgrade pip

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip3 install wheel && \
    pip3 install --no-cache-dir -U -r requirements.txt

# Set working directory and copy the application code
WORKDIR /app
COPY . .

# Expose port for Render
EXPOSE 8000

# Start Flask (for Render port) and the bot together
CMD bash -c "flask run -h 0.0.0.0 -p ${PORT:-8000} & python3 -m hanzo"
