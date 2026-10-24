#!/usr/bin/env python3
"""Generate the ADMIN_PASSWORD_HASH value for .env.

Uses hashlib.pbkdf2_hmac directly — no passlib, no bcrypt, no extra dependency.
The output format matches app/auth.py:

    pbkdf2_sha256$<iterations>$<salt_hex>$<hash_hex>

Usage:
    python scripts/hash_password.py 'your-password'
    python scripts/hash_password.py            # prompts, input hidden
    python scripts/hash_password.py --iterations 400000 'pw'

Then paste the printed line into .env as ADMIN_PASSWORD_HASH=...
"""
from __future__ import annotations

import argparse
import getpass
import hashlib
import secrets
import sys

DEFAULT_ITERATIONS = 240_000


def hash_password(password: str, iterations: int = DEFAULT_ITERATIONS) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return f"pbkdf2_sha256${iterations}${salt.hex()}${digest.hex()}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate a DayHost password hash.")
    parser.add_argument("password", nargs="?", help="password (omit to be prompted)")
    parser.add_argument(
        "--iterations", type=int, default=DEFAULT_ITERATIONS,
        help=f"PBKDF2 iterations (default {DEFAULT_ITERATIONS})",
    )
    args = parser.parse_args(argv)

    password = args.password
    if not password:
        password = getpass.getpass("Password: ")
        confirm = getpass.getpass("Confirm : ")
        if password != confirm:
            print("error: passwords do not match", file=sys.stderr)
            return 1
    if not password:
        print("error: empty password", file=sys.stderr)
        return 1

    encoded = hash_password(password, args.iterations)
    print(f"ADMIN_PASSWORD_HASH={encoded}")
    print()
    print("# Also set a long random SESSION_SECRET, e.g.:")
    print('#   python -c "import secrets;print(secrets.token_urlsafe(48))"')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
