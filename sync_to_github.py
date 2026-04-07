#!/usr/bin/env python3
"""
sync_to_github.py — Publica nova versao do SimplificaE no GitHub.
Execute sempre que quiser que os operadores recebam uma atualizacao:
    python sync_to_github.py
    python sync_to_github.py "descricao da mudanca"
"""
import shutil, subprocess, sys, os, hashlib, time
from datetime import datetime

SOURCE   = r"C:\Users\GJ166041\.wolf\skills\layout-converter-ar\webapp.py"
PROD     = r"C:\Users\GJ166041\Desktop\simplificae-prod"
DEST     = os.path.join(PROD, "webapp.py")
RAIZES   = r"C:\Users\GJ166041\.wolf\skills\layout-converter-ar\raizes_conhecidas.json"
RAIZES_D = os.path.join(PROD, "raizes_conhecidas.json")
VERSION  = os.path.join(PROD, "VERSION")

def run(cmd, cwd=None, check=True):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=cwd)
    if check and result.returncode != 0:
        print(f"ERRO: {result.stderr}")
        sys.exit(1)
    return result.stdout.strip()

def md5(path):
    with open(path, 'rb') as f:
        return hashlib.md5(f.read()).hexdigest()

print("=" * 50)
print("  SimplificaE — Publicar nova versao")
print("=" * 50)
print()

# Descricao opcional
desc = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else ""

# Verificar se mudou
if os.path.exists(DEST) and md5(SOURCE) == md5(DEST):
    print("Sem mudancas no webapp.py.")
    print("Nenhuma publicacao necessaria.")
    sys.exit(0)

# Copiar arquivos
print(f"Copiando webapp.py ({os.path.getsize(SOURCE)//1024}KB)...")
shutil.copy2(SOURCE, DEST)
shutil.copy2(RAIZES, RAIZES_D)

# Atualizar VERSION com timestamp
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
with open(VERSION, 'w') as f:
    f.write(ts + "\n")
print(f"Versao: {ts}")

# Commit e push
msg = f"deploy: {desc or 'atualizacao ' + datetime.now().strftime('%d/%m/%Y %H:%M')}"
print(f"\nPublicando: '{msg}'")

run(f'git add webapp.py raizes_conhecidas.json VERSION', cwd=PROD)
status = run('git status --short', cwd=PROD)
if not status:
    print("Nenhuma mudanca para commitar.")
    sys.exit(0)

run(f'git commit -m "{msg}"', cwd=PROD)
print("Enviando para GitHub...")
run("git push origin main", cwd=PROD)

print()
print("=" * 50)
print("PUBLICADO COM SUCESSO!")
print()
print("Os operadores receberao a atualizacao automaticamente")
print("na proxima vez que abrirem o SimplificaE.exe")
print()
print(f"Versao publicada: {ts}")
print("=" * 50)
