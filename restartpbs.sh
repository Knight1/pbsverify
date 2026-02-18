#!/bin/bash

max_retries=10
delay=1

check_pong() {
    resp=$(curl -s -k https://localhost:8007/api2/json/ping)
    echo "$resp" | grep -q '"pong":true'
}

for i in $(seq 1 $max_retries); do
    if check_pong; then
        exit 0
    fi

    sleep $delay
    delay=$((delay * 2))
done

systemctl restart proxmox-backup-proxy.service
