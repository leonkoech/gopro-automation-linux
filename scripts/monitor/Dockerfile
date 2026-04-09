FROM python:3.11-slim

# Install Tailscale
RUN apt-get update && \
    apt-get install -y curl iptables && \
    curl -fsSL https://tailscale.com/install.sh | sh && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY monitor_jetsons.py /app/monitor_jetsons.py
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

WORKDIR /app
ENTRYPOINT ["/app/entrypoint.sh"]
