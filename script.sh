#!/bin/bash

# Function to find and kill processes using a given port
kill_processes_using_port() {
    local port="$1"
    local process_ids=$(sudo lsof -ti ":$port")
    if [ -n "$process_ids" ]; then
        echo "Port $port is being used by the following process(es): $process_ids"
        sudo kill -9 $process_ids
        echo "Killed process(es) using port $port"
    else
        echo "No process is using port $port"
    fi
}

# Iterate over the provided ports and kill processes if any
for port in "$@"; do
    kill_processes_using_port $port
done
