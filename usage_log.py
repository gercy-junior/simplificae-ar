"""
usage_log.py — Telemetria de uso do SimplificaÊ.

Registra cada evento de geração e envio de e-mail em:
  ~/SimplificaE_data/logs/<operador_slug>/usage_YYYY_MM.jsonl

Formato JSON Lines (append-only): 1 evento por linha.
Os arquivos de log são empurrados pro GitHub via sync_logs.py.
"""

import os
import sys
import json
import socket
import threading
from datetime import datetime

# ---------------------------------------------------------------------------
# Diretório de dados estável (mesmo critério do webapp.py)
# ---------------------------------------------------------------------------
if getattr(sys, 'frozen', False):
    _DATA_DIR = os.path.join(os.path.expanduser('~'), 'SimplificaE_data')
else:
    _DATA_DIR = os.path.dirname(os.path.abspath(__file__))

LOGS_DIR = os.path.join(_DATA_DIR, 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)

# Versão atual — lida do arquivo VERSION junto ao executável ou ao script
def _read_version():
    candidates = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), 'VERSION'),
        os.path.join(os.path.dirname(sys.executable), 'VERSION') if getattr(sys, 'frozen', False) else None,
    ]
    for path in candidates:
        if path and os.path.exists(path):
            try:
                return open(path).read().strip()
            except Exception:
                pass
    return 'desconhecida'

APP_VERSION = _read_version()
MACHINE     = socket.gethostname()

_lock = threading.Lock()


def _operador_slug(operador_email: str) -> str:
    """cesar.oda@picpay.com → cesar_oda_picpay_com"""
    return operador_email.replace('@', '_').replace('.', '_').replace('-', '_').lower()


def _log_path(operador_email: str) -> str:
    slug     = _operador_slug(operador_email)
    mes      = datetime.now().strftime('%Y_%m')
    op_dir   = os.path.join(LOGS_DIR, slug)
    os.makedirs(op_dir, exist_ok=True)
    return os.path.join(op_dir, f'usage_{mes}.jsonl')


def registrar(
    *,
    operador: str,
    evento: str,            # "generate" | "generate_custom" | "send_email"
    empresa: str = '',
    urs: int = 0,
    valor_bruto: float = 0.0,
    valor_operavel: float = 0.0,
    taxa: float = 0.0,
    duracao_s: float = 0.0,
    status: str = 'ok',    # "ok" | "erro"
    detalhe: str = '',     # mensagem de erro opcional
    session_id: str = '',
):
    """
    Registra um evento de uso. Thread-safe, não-bloqueante.
    Falhas são silenciosas para não impactar o fluxo do operador.
    """
    entry = {
        'timestamp':      datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
        'operador':       operador,
        'maquina':        MACHINE,
        'versao':         APP_VERSION,
        'evento':         evento,
        'empresa':        empresa,
        'urs':            urs,
        'valor_bruto':    round(valor_bruto, 2),
        'valor_operavel': round(valor_operavel, 2),
        'taxa':           round(taxa, 4),
        'duracao_s':      round(duracao_s, 1),
        'status':         status,
        'detalhe':        detalhe,
        'session_id':     session_id,
    }

    def _write():
        try:
            path = _log_path(operador or 'anonimo')
            with _lock:
                with open(path, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(entry, ensure_ascii=False) + '\n')
        except Exception:
            pass  # telemetria nunca pode quebrar o app

    threading.Thread(target=_write, daemon=True).start()


# ---------------------------------------------------------------------------
# Funções auxiliares para o relatório (usadas pelo sync_logs.py)
# ---------------------------------------------------------------------------

def listar_logs_locais() -> list[str]:
    """Retorna lista de todos os arquivos .jsonl locais."""
    result = []
    for root, _, files in os.walk(LOGS_DIR):
        for f in files:
            if f.endswith('.jsonl'):
                result.append(os.path.join(root, f))
    return result


def ler_eventos(path: str) -> list[dict]:
    """Lê todos os eventos de um arquivo .jsonl."""
    eventos = []
    try:
        with open(path, encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        eventos.append(json.loads(line))
                    except Exception:
                        pass
    except Exception:
        pass
    return eventos
