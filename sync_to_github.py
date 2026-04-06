#!/usr/bin/env python3
"""
sync_to_github.py — Sincroniza o webapp.py local para o repositório de producao.
Execute sempre que quiser publicar uma nova versao:
    python sync_to_github.py
"""
import shutil
import subprocess
import sys
import os
from datetime import datetime

# Paths
SOURCE = r"C:\Users\GJ166041\.wolf\skills\layout-converter-ar\webapp.py"
PROD_REPO = r"C:\Users\GJ166041\Desktop\simplificae-prod"
DEST = os.path.join(PROD_REPO, "webapp.py")

# Tambem sincronizar raizes
RAIZES_SRC = r"C:\Users\GJ166041\.wolf\skills\layout-converter-ar\raizes_conhecidas.json"
RAIZES_DST = os.path.join(PROD_REPO, "raizes_conhecidas.json")

def run(cmd, cwd=None):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=cwd)
    if result.returncode != 0:
        print(f"ERRO: {result.stderr}")
        sys.exit(1)
    return result.stdout.strip()

print("=== SimplificaE — Sync para Producao ===")
print()

# 1. Copiar webapp.py
print(f"Copiando webapp.py ({os.path.getsize(SOURCE)//1024}KB)...")
shutil.copy2(SOURCE, DEST)

# 2. Copiar raizes
shutil.copy2(RAIZES_SRC, RAIZES_DST)
print("Copiando raizes_conhecidas.json...")

# 3. Commit e push
ts = datetime.now().strftime("%d/%m/%Y %H:%M")
msg = f"deploy: atualizacao webapp {ts}"

print(f"\nCommitando: '{msg}'")
run("git add webapp.py raizes_conhecidas.json", cwd=PROD_REPO)

# Verificar se ha mudancas
status = run("git status --short", cwd=PROD_REPO)
if not status:
    print("Nenhuma mudanca detectada. Arquivo ja esta atualizado.")
    sys.exit(0)

run(f'git commit -m "{msg}"', cwd=PROD_REPO)
print("Fazendo push para GitHub...")
run("git push origin main", cwd=PROD_REPO)

print()
print("=" * 50)
print("DEPLOY INICIADO!")
print("Railway vai detectar o push e fazer o deploy automaticamente.")
print("Acompanhe em: https://railway.app")
print()
print("O app fica disponivel em ~2-3 minutos.")
