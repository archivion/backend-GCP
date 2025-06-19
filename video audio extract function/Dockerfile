# Use an official Node.js runtime as a parent image.
FROM node:22-slim

# Install FFmpeg for audio extraction
RUN apt-get update && apt-get install -y ffmpeg --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy package files and install dependencies
COPY package*.json ./
RUN npm ci --only=production || npm install --only=production

# Bundle app source code
COPY . .

# The Cloud Functions framework will handle the final command to start the server.
ENV PORT=8080
