from pymongo import MongoClient
from pymongo.errors import PyMongoError
import socket
import time

# =========================
# MongoDB Configuration
# =========================

MONGO_HOST = "10.100.192.123"
MONGO_PORT = 27017

MONGO_USER = "admin"
MONGO_PASS = "admin123"
AUTH_DB = "admin"

MONGO_URI = (
    f"mongodb://{MONGO_USER}:{MONGO_PASS}"
    f"@{MONGO_HOST}:{MONGO_PORT}/"
    f"?authSource={AUTH_DB}"
)

print("=" * 70)
print("MongoDB-123 Connectivity & Authentication Test")
print("=" * 70)

print(f"Source Host : {socket.gethostname()}")
print(f"Mongo Host  : {MONGO_HOST}")
print(f"Mongo Port  : {MONGO_PORT}")
print(f"Auth DB     : {AUTH_DB}")

# ============================================================
# 1. TCP CONNECTION TEST
# ============================================================

print("\n[1] Testing TCP connection...")

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.settimeout(5)

start = time.time()

try:
    sock.connect((MONGO_HOST, MONGO_PORT))
    elapsed = (time.time() - start) * 1000

    print("SUCCESS")
    print(f"TCP connection established")
    print(f"Response time: {elapsed:.2f} ms")

except Exception as e:
    print("FAILED")
    print(f"TCP connection failed: {type(e).__name__}: {e}")
    exit(1)

finally:
    sock.close()


# ============================================================
# 2. MONGODB AUTHENTICATED CONNECTION
# ============================================================

print("\n[2] Testing MongoDB authentication...")

try:
    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
        socketTimeoutMS=10000
    )

    result = client.admin.command("ping")

    print("SUCCESS")
    print(f"MongoDB ping response: {result}")

except PyMongoError as e:
    print("FAILED")
    print(f"MongoDB authentication/connection failed")
    print(f"Error type: {type(e).__name__}")
    print(f"Error: {e}")

    exit(1)


# ============================================================
# 3. SERVER VERSION
# ============================================================

print("\n[3] Checking MongoDB server version...")

try:
    version = client.server_info()["version"]

    print("SUCCESS")
    print(f"MongoDB Version: {version}")

except PyMongoError as e:
    print("FAILED")
    print(f"Error: {type(e).__name__}: {e}")


# ============================================================
# 4. LIST DATABASES
# ============================================================

print("\n[4] Checking database access...")

try:
    databases = client.list_database_names()

    print("SUCCESS")
    print("Databases available:")

    for db in databases:
        print(f"  - {db}")

except PyMongoError as e:
    print("FAILED")
    print(f"Error: {type(e).__name__}: {e}")


# ============================================================
# 5. CHECK CURRENT USER
# ============================================================

print("\n[5] Checking authenticated user...")

try:
    users = client.admin.command("usersInfo", MONGO_USER)

    print("SUCCESS")

    if users.get("users"):
        for user in users["users"]:
            print(f"User : {user.get('user')}")
            print(f"DB   : {user.get('db')}")
            print("Roles:")

            for role in user.get("roles", []):
                print(f"  - {role}")

    else:
        print("WARNING: User not found")

except PyMongoError as e:
    print("FAILED")
    print(f"Error: {type(e).__name__}: {e}")


# ============================================================
# FINISH
# ============================================================

client.close()

print("\n" + "=" * 70)
print("TEST COMPLETED")
print("=" * 70)