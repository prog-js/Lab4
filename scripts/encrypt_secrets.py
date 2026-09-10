#!/usr/bin/env python3
"""
Шифрование секретов без ansible-vault
"""
import os
import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2

def encrypt_file(input_file, output_file, password):
    """Зашифровать файл"""
    with open(input_file, 'rb') as f:
        data = f.read()
    
    # Генерируем соль
    salt = os.urandom(16)
    
    # Создаём ключ из пароля
    kdf = PBKDF2(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=100000,
    )
    key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
    
    # Шифруем
    cipher = Fernet(key)
    encrypted = cipher.encrypt(data)
    
    # Сохраняем соль + зашифрованные данные
    with open(output_file, 'wb') as f:
        f.write(salt)
        f.write(encrypted)
    
    print(f"✅ Файл зашифрован: {output_file}")

# Создаём папку vault
os.makedirs('vault', exist_ok=True)

# Создаём пример файла с секретами (если его нет)
if not os.path.exists('vault/secrets.yml'):
    sample_secrets = """database:
  host: postgres
  port: 5432
  user: ml_user
  password: StrongPassword123!
  name: ml_models

api:
  secret_key: your_super_secret_key_for_jwt_at_least_32_chars
  algorithm: HS256
"""
    with open('vault/secrets.yml', 'w', encoding='utf-8') as f:
        f.write(sample_secrets)
    print("📝 Создан пример файла vault/secrets.yml")
    print("   Отредактируйте его, затем запустите скрипт снова")
else:
    # Проверяем, есть ли файл с паролем
    if not os.path.exists('vault/vault-password.txt'):
        password = input("Введите пароль для шифрования: ")
        with open('vault/vault-password.txt', 'w') as f:
            f.write(password)
        print("✅ Создан vault/vault-password.txt")
    
    # Читаем пароль
    with open('vault/vault-password.txt', 'r') as f:
        password = f.read().strip()
    
    # Шифруем
    encrypt_file('vault/secrets.yml', 'vault/secrets.enc', password)
    
    print("\n📋 Что делать дальше:")
    print("1. Удалите незашифрованный файл: del vault\\secrets.yml")
    print("2. Добавьте в .gitignore:")
    print("   vault/secrets.yml")
    print("   vault/vault-password.txt")
    print("3. Закоммитьте зашифрованный файл: git add vault/secrets.enc")