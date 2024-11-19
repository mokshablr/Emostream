#!/bin/bash

sudo apt-get update
sudo apt-get install -y nginx

USER=$(whoami)

sudo bash -c "cat > /etc/nginx/nginx.conf <<EOF
user $USER;
worker_processes auto;
pid /var/run/nginx.pid;
include /etc/nginx/modules-enabled/*.conf;

events {
    worker_connections 1024;
}

http {
    include /etc/nginx/mime.types;
    default_type application/octet-stream;

    log_format main '\\\$remote_addr - \\\$remote_user [\\\$time_local] \"\\\$request\" '
                    '\\\$status \\\$body_bytes_sent \"\\\$http_referer\" '
                    '\"\\\$http_user_agent\" \"\\\$http_x_forwarded_for\"';

    access_log /var/log/nginx/access.log main;

    sendfile on;
    tcp_nopush on;
    tcp_nodelay on;
    keepalive_timeout 65;
    types_hash_max_size 4096;

    upstream flask_servers {
        server 0.0.0.0:5000;
        server 0.0.0.0:5001;
    }

    server {
        listen 80;

        location / {
            proxy_pass http://flask_servers\\\$request_uri;
            proxy_set_header Host \\\$host;
            proxy_set_header X-Real-IP \\\$remote_addr;
            proxy_set_header X-Forwarded-For \\\$proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto \\\$scheme;
        }
    }

    include /etc/nginx/conf.d/*.conf;
}
EOF"

# Restart NGINX to apply the new configuration
sudo systemctl restart nginx

echo "NGINX installed and configured successfully."

