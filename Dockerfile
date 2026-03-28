# Build stage
FROM ubuntu:24.04 AS builder

# Avoid prompts from apt
ENV DEBIAN_FRONTEND=noninteractive

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    libpq-dev \
    ca-certificates \
    lsb-release \
    wget \
    gnupg \
    && wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
    && apt-get install -y ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
    && apt-get update && apt-get install -y \
    libarrow-dev \
    libparquet-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy source
COPY . .

# Build
RUN rm -rf build && mkdir build && cd build && \
    cmake .. && \
    make -j$(nproc)

# Final stage
FROM ubuntu:24.04

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libpq5 \
    ca-certificates \
    lsb-release \
    wget \
    gnupg \
    && wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
    && apt-get install -y ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
    && apt-get update && apt-get install -y \
    libarrow-dev \
    libparquet-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/build/pg_delta_lake_cdc .

# Create output directory
RUN mkdir -p data

# Default command
CMD ["./pg_delta_lake_cdc"]
