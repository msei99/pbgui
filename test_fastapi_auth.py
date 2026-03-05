#!/usr/bin/env python3
"""Quick test script for FastAPI auth and job monitor."""

import requests
import sys
from pathlib import Path

# Add current dir to path
sys.path.insert(0, str(Path(__file__).parent))

from api.auth import generate_token

# Generate test token
print("=" * 60)
print("FastAPI Auth & Job Monitor Test")
print("=" * 60)

user_id = "test_user"
token_obj = generate_token(user_id, expires_in_seconds=3600)
token = token_obj.token

print(f"\n✅ Generated token: {token[:16]}...{token[-16:]}")
print(f"   User: {user_id}")
print(f"   Expires in: 1 hour\n")

# Test 1: Health endpoint (no auth needed)
print("Test 1: Health endpoint...")
resp = requests.get("http://localhost:8000/health")
if resp.status_code == 200:
    print(f"   ✅ Health OK: {resp.json()}")
else:
    print(f"   ❌ Health failed: {resp.status_code}")

# Test 2: API without token (should fail)
print("\nTest 2: API without token (expect 401)...")
resp = requests.get("http://localhost:8000/api/jobs/")
if resp.status_code == 401:
    print(f"   ✅ Auth required: {resp.json()['detail']}")
else:
    print(f"   ❌ Unexpected status: {resp.status_code}")

# Test 3: API with token query param
print("\nTest 3: API with token query param...")
resp = requests.get(f"http://localhost:8000/api/jobs/?token={token}")
if resp.status_code == 200:
    data = resp.json()
    print(f"   ✅ Jobs retrieved: {len(data.get('jobs', []))} jobs")
    print(f"   Worker running: {data.get('worker_running')}")
else:
    print(f"   ❌ Failed: {resp.status_code} - {resp.text[:100]}")

# Test 4: API with Bearer header
print("\nTest 4: API with Bearer header...")
headers = {"Authorization": f"Bearer {token}"}
resp = requests.get("http://localhost:8000/api/jobs/", headers=headers)
if resp.status_code == 200:
    data = resp.json()
    print(f"   ✅ Jobs retrieved: {len(data.get('jobs', []))} jobs")
else:
    print(f"   ❌ Failed: {resp.status_code}")

# Test 5: Frontend HTML
print("\nTest 5: Frontend HTML...")
resp = requests.get("http://localhost:8000/app/jobs_monitor.html")
if resp.status_code == 200 and "<title>Job Monitor" in resp.text:
    print(f"   ✅ Frontend accessible ({len(resp.text)} bytes)")
else:
    print(f"   ❌ Frontend failed: {resp.status_code}")

print("\n" + "=" * 60)
print("Test complete! Use this URL in Streamlit iframe:")
print(f"\n  http://localhost:8000/app/jobs_monitor.html?token={token}")
print("\n" + "=" * 60)
