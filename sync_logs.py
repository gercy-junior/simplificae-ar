#!/usr/bin/env python3
"""
sync_logs.py — Envia logs de uso do SimplificaÊ para o repositório GitHub.

Execute sempre que quiser subir os dados de uso:
    python sync_logs.py

O script:
  1. Copia logs de ~/SimplificaE_data/logs/ → <repo>/logs/
  2. Faz git add + commit + push no repo simplificae-prod
  3. Exibe resumo do que foi sincronizado

Os logs ficam em:
  <repo>/logs/<operador_slug>/usage_YYYY_MM.jsonl
"""

import os
import sys
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
PROD = Path(r"C:\Users\GJ166041\Desktop\simplificae-prod")

if getattr(sys, 'frozen', False):
    _DATA_DIR = Path(os.path.expanduser('~')) / 'SimplificaE_data'
else:
    # Quando rodado como script, tenta detectar pelo irmão usage_log.py
    _script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    _DATA_DIR = Path(os.path.expanduser('~')) / 'SimplificaE_data'

LOCAL_LOGS = _DATA_DIR / 'logs'
REPO_LOGS  = PROD / 'logs'


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def run(cmd: str, cwd=None, check=True):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=cwd)
    if check and r.returncode != 0:
        print(f"  ERRO ({r.returncode}): {r.stderr.strip()}")
        sys.exit(1)
    return r.stdout.strip()


def copiar_logs() -> int:
    """Copia todos os .jsonl de LOCAL_LOGS para REPO_LOGS. Retorna nº de arquivos."""
    if not LOCAL_LOGS.exists():
        print("  Nenhum log local encontrado.")
        return 0

    copiados = 0
    for src in LOCAL_LOGS.rglob('*.jsonl'):
        rel    = src.relative_to(LOCAL_LOGS)
        dst    = REPO_LOGS / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        copiados += 1
        print(f"  → {rel}")

    return copiados


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
print("=" * 50)
print("  SimplificaÊ — Sync de Logs")
print("=" * 50)
print()

if not PROD.exists():
    print(f"Repo não encontrado: {PROD}")
    sys.exit(1)

# Verificar se tem git
try:
    run("git --version", check=True)
except SystemExit:
    print("Git não encontrado. Instale o Git para sincronizar logs.")
    sys.exit(1)

# Copiar logs
print("Copiando logs locais para o repo...")
n = copiar_logs()
if n == 0:
    print("Nada a sincronizar.")
    sys.exit(0)

print(f"\n{n} arquivo(s) copiado(s).\n")

# git status — ver se há mudanças
status = run("git status --short logs/", cwd=PROD, check=False)
if not status.strip():
    print("Logs já estão atualizados no repo. Nada a commitar.")
    sys.exit(0)

print("Mudanças detectadas:")
print(status)
print()

# git add + commit + push
ts     = datetime.now().strftime("%Y-%m-%d %H:%M")
msg    = f"logs: sync {ts}"

print("Commitando...")
run("git add logs/", cwd=PROD)
run(f'git commit -m "{msg}"', cwd=PROD)

print("Fazendo push...")
run("git push", cwd=PROD)

print(f"\n✓ {n} arquivo(s) sincronizados com sucesso.")
