#!/usr/bin/env python3
"""
sync_to_github.py — Publica nova versao do SimplificaE no GitHub.
Execute sempre que quiser que os operadores recebam uma atualizacao:
    python sync_to_github.py
    python sync_to_github.py "descricao da mudanca"
"""
import shutil, subprocess, sys, os, hashlib, time
from datetime import datetime

SKILL    = r"C:\Users\GJ166041\.wolf\skills\layout-converter-ar"
PROD     = r"C:\Users\GJ166041\Desktop\simplificae-prod"
VERSION  = os.path.join(PROD, "VERSION")

# Arquivos principais do app
ARQUIVOS = [
    ("webapp.py",          os.path.join(SKILL, "webapp.py"),          os.path.join(PROD, "webapp.py")),
    ("raizes.json",        os.path.join(SKILL, "raizes_conhecidas.json"), os.path.join(PROD, "raizes_conhecidas.json")),
    ("usage_log.py",       os.path.join(SKILL, "usage_log.py"),       os.path.join(PROD, "usage_log.py")),
    ("updater.py",         os.path.join(SKILL, "updater.py"),         os.path.join(PROD, "updater.py")),
    ("sync_logs.py",       os.path.join(SKILL, "sync_logs.py"),       os.path.join(PROD, "sync_logs.py")),
]

def run(cmd, cwd=None, check=True):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=cwd)
    if check and result.returncode != 0:
        print(f"ERRO: {result.stderr}")
        sys.exit(1)
    return result.stdout.strip()

def md5(path):
    with open(path, 'rb') as f:
        return hashlib.md5(f.read()).hexdigest()

def changed(src, dst):
    if not os.path.exists(dst):
        return True
    return md5(src) != md5(dst)

print("=" * 50)
print("  SimplificaE — Publicar nova versao")
print("=" * 50)
print()

# Descricao opcional
desc = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else ""

# Verificar mudancas
houve_mudanca = any(changed(src, dst) for (_, src, dst) in ARQUIVOS if os.path.exists(src))
if not houve_mudanca:
    print("Sem mudancas detectadas em nenhum arquivo.")
    print("Nenhuma publicacao necessaria.")
    sys.exit(0)

# Copiar arquivos com mudancas
print("Copiando arquivos alterados...")
arquivos_git = []
for nome, src, dst in ARQUIVOS:
    if not os.path.exists(src):
        print(f"  [SKIP] {nome} — nao encontrado em {src}")
        continue
    status = "NOVO" if not os.path.exists(dst) else ("ATUALIZADO" if changed(src, dst) else "SEM MUDANCA")
    if status != "SEM MUDANCA":
        shutil.copy2(src, dst)
        arquivos_git.append(os.path.basename(dst))
        print(f"  [{status}] {nome} ({os.path.getsize(src)//1024}KB)")
    else:
        print(f"  [OK]    {nome} — sem alteracoes")

# Garantir pasta logs no repo (com .gitkeep)
logs_dir = os.path.join(PROD, "logs")
os.makedirs(logs_dir, exist_ok=True)
gitkeep  = os.path.join(logs_dir, ".gitkeep")
if not os.path.exists(gitkeep):
    open(gitkeep, 'w').close()
    arquivos_git.append("logs/.gitkeep")

# Atualizar VERSION com timestamp
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
with open(VERSION, 'w') as f:
    f.write(ts + "\n")
arquivos_git.append("VERSION")
print(f"\nVersao: {ts}")

# git add e commit
add_targets = " ".join(arquivos_git) + " VERSION"
msg = f"deploy: {desc or 'atualizacao ' + datetime.now().strftime('%d/%m/%Y %H:%M')}"
print(f"\nPublicando: '{msg}'")

run(f'git add {add_targets}', cwd=PROD)
status_git = run('git status --short', cwd=PROD)
if not status_git:
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
print()
print("Arquivos publicados:")
for a in arquivos_git:
    print(f"  - {a}")
print("=" * 50)
