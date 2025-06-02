import hashlib
import os
import json
from typing import Dict, Tuple, List

USER_FILE = "users.json"

class AuthManager:
    def __init__(self):
        self.users: Dict[str, Tuple[str, str, str, bool]] = {}
        self._load_users_from_file()

        # Ensure default users exist
        for uname, pwd, role in [
            ("admin", "admin123", "full"),
            ("legato", "legato123", "limited"),
            ("business", "business123", "view_only")
        ]:
            if uname not in self.users:
                self.users[uname] = (*self._create_user(uname, pwd, role), True)
                self._save_users_to_file()

    def _create_user(self, username: str, password: str, access_level: str) -> Tuple[str, str, str]:
        """Securely registers a new user"""
        salt = self._generate_salt()
        hashed_pw = self._hash_password(password, salt)
        return (access_level, salt, hashed_pw)

    @staticmethod
    def _generate_salt() -> str:
        """Generates 16-byte random salt"""
        return hashlib.sha256(os.urandom(60)).hexdigest()

    @staticmethod
    def _hash_password(password: str, salt: str) -> str:
        """SHA-256 with salt and 100,000 iterations"""
        return hashlib.pbkdf2_hmac(
            'sha256',
            password.encode('utf-8'),
            salt.encode('utf-8'),
            100000
        ).hex()

    def authenticate(self, username: str, password: str) -> Dict[str, any]:
        """Secure authentication"""
        user_data = self.users.get(username.lower())
        if not user_data:
            return {"authenticated": False}

        access_level, salt, stored_hash, *_ = user_data
        attempt_hash = self._hash_password(password, salt)

        if self._secure_compare(attempt_hash, stored_hash):
            return {
                "authenticated": True,
                "access_level": access_level
            }
        return {"authenticated": False}

    @staticmethod
    def _secure_compare(a: str, b: str) -> bool:
        """Timing-attack resistant comparison"""
        return len(a) == len(b) and all(x == y for x, y in zip(a, b))

    def add_user(self, username: str, password: str, role: str) -> bool:
        """Add a new user (admin-only)"""
        if username.lower() in self.users:
            return False
        self.users[username.lower()] = (*self._create_user(username, password, role), True)
        self._save_users_to_file()
        return True

    def update_user_role(self, username: str, new_role: str) -> bool:
        """Update an existing user's role"""
        if username.lower() not in self.users:
            return False
        old_data = self.users[username.lower()]
        self.users[username.lower()] = (new_role, old_data[1], old_data[2], old_data[3])
        self._save_users_to_file()
        return True

    def list_users(self) -> List[Dict]:
        """List all users with their roles and active status"""
        return [{
            "username": uname,
            "role": data[0],
            "is_active": data[3]
        } for uname, data in self.users.items()]

    def toggle_user_active(self, username: str) -> bool:
        """Enable or disable a user"""
        if username.lower() not in self.users:
            return False
        old_data = self.users[username.lower()]
        self.users[username.lower()] = (*old_data[:3], not old_data[3])
        self._save_users_to_file()
        return True

    def _save_users_to_file(self):
        """Save users to a JSON file"""
        with open(USER_FILE, "w") as f:
            json.dump(self.users, f)

    def _load_users_from_file(self):
        """Load users from a JSON file"""
        if os.path.exists(USER_FILE):
            with open(USER_FILE, "r") as f:
                raw = json.load(f)
                self.users = {
                    uname: tuple(val) for uname, val in raw.items()
                }

    def update_user_password(self, username: str, new_password: str) -> bool:
        if username.lower() not in self.users:
            return False
        role, _, _, is_active = self.users[username.lower()]
        salt = self._generate_salt()
        hashed = self._hash_password(new_password, salt)
        self.users[username.lower()] = (role, salt, hashed, is_active)
        self._save_users_to_file()  # Ensure it's saved to JSON
        return True

    def delete_user(self, username: str) -> bool:
        uname = username.lower()
        if uname in self.users:
            del self.users[uname]
            self._save_users_to_file()  # Persist the change
            return True
        return False

