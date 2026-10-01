import os


# ============================================================
# KOTAK NEO API CONFIGURATION
# ============================================================

CONSUMER_KEY = os.environ.get(
    "CONSUMER_KEY",
    ""
)

MOBILE_NUMBER = os.environ.get(
    "MOBILE_NUMBER",
    ""
)

UCC = os.environ.get(
    "UCC",
    ""
)

TOTP = os.environ.get(
    "TOTP",
    ""
)

MPIN = os.environ.get(
    "MPIN",
    ""
)


# ============================================================
# VALIDATE CONFIGURATION
# ============================================================

missing = []

if not CONSUMER_KEY:
    missing.append("CONSUMER_KEY")

if not MOBILE_NUMBER:
    missing.append("MOBILE_NUMBER")

if not UCC:
    missing.append("UCC")

if not TOTP:
    missing.append("TOTP")

if not MPIN:
    missing.append("MPIN")


if missing:

    print(
        "WARNING: Missing environment variables:"
    )

    for item in missing:
        print(
            f"  - {item}"
        )
