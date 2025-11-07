#!/usr/bin/env bash
# Master VPS initialization script
# Steps:
# 1. Set hostname
# 2. Create user with sudo privileges
# 3. Disable direct root login
set -euo pipefail

# --- Check Ubuntu ---
if [ "$(lsb_release -si)" != "Ubuntu" ]; then
    echo "This script only works on Ubuntu."
    exit 1
fi

# --- Args ---
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <server_name> <user_name>"
    exit 1
fi

SERVER_NAME="$1"
USER_NAME="$2"

# --- Prompt user password for local account ---
user_pw=""
for attempt in 1 2 3; do
    read -s -p "Enter password for user '$USER_NAME': " pw1
    echo
    read -s -p "Confirm password: " pw2
    echo
    if [ "$pw1" = "$pw2" ] && [ -n "$pw1" ]; then
        user_pw="$pw1"
        break
    fi
    echo "Passwords do not match or are empty. Attempts left: $((3 - attempt))"
done
if [ -z "$user_pw" ]; then
    echo "Failed to read a valid user password."
    exit 1
fi

# --- Set hostname ---
if [ "$(id -u)" -eq 0 ]; then
    hostnamectl set-hostname "$SERVER_NAME"
else
    printf "%s\n" "$user_pw" | sudo -S hostnamectl set-hostname "$SERVER_NAME"
fi
echo "Hostname changed to '$SERVER_NAME'."

# --- Create user ---
if ! id "$USER_NAME" &>/dev/null; then
    sudo useradd -m -s /bin/bash "$USER_NAME"
    echo "$USER_NAME:$user_pw" | sudo chpasswd
    sudo usermod -aG sudo "$USER_NAME"
    echo "User $USER_NAME created."
else
    echo "User $USER_NAME already exists."
fi

# --- Disable direct root login ---
if [ "$(id -u)" -eq 0 ]; then
    sudo passwd -l root
    sudo sed -i 's/^PermitRootLogin.*/PermitRootLogin no/' /etc/ssh/sshd_config
    if ! grep -q '^PermitRootLogin' /etc/ssh/sshd_config; then
        echo 'PermitRootLogin no' | sudo tee -a /etc/ssh/sshd_config > /dev/null
    fi
    if systemctl list-units --type=service | grep -q 'sshd.service'; then
        sudo systemctl reload sshd
    else
        sudo systemctl reload ssh
    fi
    echo "Root login disabled."
fi
echo "Master VPS initialization complete."