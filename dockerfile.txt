# Use official Node.js runtime with GDAL pre-installed
FROM osgeo/gdal:ubuntu-small-3.8.0

# Install Node.js and build tools
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    python3 \
    python3-dev \
    && curl -fsSL https://deb.nodesource.com/setup_18.x | bash - \
    && apt-get install -y nodejs \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy source code
COPY . .

# Create uploads directory
RUN mkdir -p uploads

# Expose port (Railway will set the PORT environment variable)
EXPOSE $PORT

# Start the server
CMD ["npm", "start"]