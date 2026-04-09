"""
usage_log.py — Telemetria de uso do SimplificaÊ.

Registra cada evento de geração e envio de e-mail em:
  ~/SimplificaE_data/logs/<operador_slug>/usage_YYYY_MM.jsonl

Formato JSON Lines (append-only): 1 evento por linha.
Push automático para GitHub em background após cada evento —
mesmo mecanismo do histórico de cotações.
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

# Repositório GitHub — mesmo do histórico
GITHUB_REPO = 'gercy-junior/simplificae-ar'
GITHUB_API  = 'https://api.github.com'

_lock = threading.Lock()


def _read_version() -> str:
    candidates = [
        os.path.join(os.path.dirname(sys.executable), 'VERSION') if getattr(sys, 'frozen', False) else None,
        os.path.join(os.path.dirname(sys.executable), '_internal', 'VERSION') if getattr(sys, 'frozen', False) else None,
        os.path.join(os.path.dirname(os.path.abspath(__file__)), 'VERSION'),
    ]
    for path in candidates:
        if path and os.path.exists(path):
            try:
                v = open(path).read().strip()
                if v:
                    return v
            except Exception:
                pass
    return 'desconhecida'


def _get_token() -> str:
    """Lê o GITHUB_METRICS_TOKEN do ambiente ou do .env."""
    token = os.environ.get('GITHUB_METRICS_TOKEN', '')
    if token:
        return token
    # Tentar do .env
    for env_dir in [
        os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else None,
        os.path.join(os.path.dirname(sys.executable), '_internal') if getattr(sys, 'frozen', False) else None,
        os.path.dirname(os.path.abspath(__file__)),
    ]:
        if not env_dir:
            continue
        env_path = os.path.join(env_dir, '.env')
        if os.path.exists(env_path):
            try:
                with open(env_path, encoding='utf-8', errors='ignore') as f:
                    for line in f:
                        if line.startswith('GITHUB_METRICS_TOKEN='):
                            return line.split('=', 1)[1].strip()
            except Exception:
                pass
    return ''


APP_VERSION = _read_version()
MACHINE     = socket.gethostname()


def _operador_slug(operador_email: str) -> str:
    """cesar.oda@picpay.com → cesar_oda_picpay_com"""
    import re
    return re.sub(r'[^a-z0-9]', '_', operador_email.lower())


def _log_path(operador_email: str) -> str:
    slug   = _operador_slug(operador_email or 'anonimo')
    mes    = datetime.now().strftime('%Y_%m')
    op_dir = os.path.join(LOGS_DIR, slug)
    os.makedirs(op_dir, exist_ok=True)
    return os.path.join(op_dir, f'usage_{mes}.jsonl')


# ---------------------------------------------------------------------------
# Push automático para GitHub (mesmo padrão do histórico de cotações)
# ---------------------------------------------------------------------------

def _push_log_github(operador: str, entry: dict):
    """
    Envia o log de telemetria para o GitHub em background.
    Cria/atualiza: logs/<operador_slug>/usage_YYYY_MM.jsonl
    Falha silenciosamente — nunca impacta o operador.
    """
    try:
        import base64, ssl, urllib.request

        # Verificar conectividade rápida
        try:
            socket.create_connection(('api.github.com', 443), timeout=3)
        except Exception:
            return  # sem internet

        token = _get_token()
        if not token:
            return  # sem token, não sincronizar

        slug     = _operador_slug(operador or 'anonimo')
        mes      = datetime.now().strftime('%Y_%m')
        filename = f'logs/{slug}/usage_{mes}.jsonl'
        api_url  = f'{GITHUB_API}/repos/{GITHUB_REPO}/contents/{filename}'

        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode    = ssl.CERT_NONE

        headers = {
            'Authorization': f'token {token}',
            'Accept':        'application/vnd.github.v3+json',
            'Content-Type':  'application/json',
            'User-Agent':    'SimplificaE-UsageLog/1.0',
        }

        # GET para obter SHA e conteúdo atual
        existing_lines = []
        sha = None
        try:
            req = urllib.request.Request(api_url, headers=headers)
            with urllib.request.urlopen(req, context=ctx, timeout=8) as r:
                resp         = json.loads(r.read())
                sha          = resp.get('sha', '')
                content_b64  = resp.get('content', '').replace('\n', '')
                if content_b64:
                    raw = base64.b64decode(content_b64).decode('utf-8')
                    existing_lines = [l for l in raw.splitlines() if l.strip()]
        except Exception:
            pass  # arquivo ainda não existe

        # Acrescentar nova linha
        new_line = json.dumps(entry, ensure_ascii=False)
        existing_lines.append(new_line)

        # Limitar a 5000 linhas por arquivo mensal
        if len(existing_lines) > 5000:
            existing_lines = existing_lines[-5000:]

        new_content = base64.b64encode(
            '\n'.join(existing_lines).encode('utf-8')
        ).decode('utf-8')

        body = {
            'message': f'logs: {entry.get("evento","uso")} {operador}',
            'content': new_content,
        }
        if sha:
            body['sha'] = sha

        req2 = urllib.request.Request(
            api_url,
            data=json.dumps(body).encode('utf-8'),
            headers=headers,
            method='PUT',
        )
        with urllib.request.urlopen(req2, context=ctx, timeout=12) as r:
            pass  # sucesso

    except Exception:
        pass  # sempre silencioso


def _push_background(operador: str, entry: dict):
    """Dispara push em thread daemon — não bloqueia o operador."""
    threading.Thread(
        target=_push_log_github,
        args=(operador, entry),
        daemon=True
    ).start()


# ---------------------------------------------------------------------------
# API pública
# ---------------------------------------------------------------------------

def registrar(
    *,
    operador: str,
    evento: str,
    empresa: str = '',
    urs: int = 0,
    valor_bruto: float = 0.0,
    valor_operavel: float = 0.0,
    taxa: float = 0.0,
    duracao_s: float = 0.0,
    status: str = 'ok',
    detalhe: str = '',
    session_id: str = '',
):
    """
    Registra um evento de uso.
    - Grava localmente em ~/SimplificaE_data/logs/ (síncrono em thread)
    - Empurra para GitHub em background (assíncrono, silencioso)
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

    def _write_and_push():
        # 1. Gravar local
        try:
            path = _log_path(operador or 'anonimo')
            with _lock:
                with open(path, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(entry, ensure_ascii=False) + '\n')
        except Exception:
            pass

        # 2. Push GitHub em background (thread separada para não bloquear a escrita local)
        _push_background(operador, entry)

    threading.Thread(target=_write_and_push, daemon=True).start()


# ---------------------------------------------------------------------------
# Funções auxiliares (usadas por relatórios e sync_logs.py)
# ---------------------------------------------------------------------------

def listar_logs_locais() -> list:
    result = []
    for root, _, files in os.walk(LOGS_DIR):
        for f in files:
            if f.endswith('.jsonl'):
                result.append(os.path.join(root, f))
    return result


def ler_eventos(path: str) -> list:
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
