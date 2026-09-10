import os
import base64
import yaml
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes

def decrypt_file(file_path, password):
    """Расшифровать файл с секретами"""
    with open(file_path, 'rb') as f:
        salt = f.read(16)
        encrypted = f.read()
    
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=100000,
    )
    key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
    cipher = Fernet(key)
    decrypted = cipher.decrypt(encrypted)
    return yaml.safe_load(decrypted.decode('utf-8'))

class VaultClient:
    def __init__(self, vault_file='vault/secrets.enc', password_file='vault/vault-password.txt'):
        self.vault_file = vault_file
        self.password = None
        
        # Пробуем получить пароль из файла
        if os.path.exists(password_file):
            with open(password_file, 'r') as f:
                self.password = f.read().strip()
        else:
            # Или из переменной окружения (для Docker/Jenkins)
            self.password = os.environ.get('VAULT_PASSWORD')
        
        if not self.password:
            raise ValueError("Vault password not found! Set VAULT_PASSWORD env var or create vault/vault-password.txt")
        
        # Расшифровываем секреты
        self.secrets = decrypt_file(vault_file, self.password)
    
    def get_db_config(self):
        return self.secrets.get('database', {})
    
    def get_api_config(self):
        return self.secrets.get('api', {})
    
    def get_secret(self, key, default=None):
        keys = key.split('.')
        value = self.secrets
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return default
        return value if value is not None else default