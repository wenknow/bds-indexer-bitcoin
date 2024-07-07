# Use the official Python 3.11 image
FROM python:3.11

# Set the working directory
WORKDIR /blockchain-data-subnet-indexer-bitcoin

# Copy the requirements file into the working directory
COPY requirements.txt requirements.txt

# Update the package list and install necessary packages
RUN apt-get update && apt-get install -y \
    python3-dev \
    cmake \
    make \
    gcc \
    g++ \
    libssl-dev

# Install pymgclient directly via pip
RUN pip install pymgclient
RUN pip install sqlalchemy

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy all remaining project files to the working directory
COPY . .

# Make the scripts executable
RUN chmod +x scripts/*

