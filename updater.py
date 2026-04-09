"""
updater.py — Auto-update do SimplificaÊ via GitHub Releases.

Fluxo:
  1. check_update_async() é chamado no startup do webapp (thread daemon)
  2. Consulta VERSION remoto no GitHub (raw URL pública, sem auth)
  3. Se versão remota > local: sinaliza _update_available = True
  4. /check_update (Flask) retorna o estado atual → frontend exibe banner
  5. /do_update baixa novo .exe, cria update.bat e o executa → app relança

O .exe não pode sobrescrever a si mesmo enquanto roda.
Solução: baixa como SimplificaE_new.exe, cria update.bat que:
  - aguarda o processo atual terminar
  - copia _new → SimplificaE.exe
  - relança SimplificaE.exe
  - se deleta
"""

import os
import sys
import time
import threading
import subprocess
import urllib.request
import urllib.error
from datetime import datetime

# ---------------------------------------------------------------------------
# Configuração — ajuste GITHUB_REPO para o repo real
# ---------------------------------------------------------------------------
GITHUB_REPO     = 'gercy-junior/simplificae-ar'
GITHUB_BRANCH   = 'main'
VERSION_URL     = f'https://raw.githubusercontent.com/{GITHUB_REPO}/{GITHUB_BRANCH}/VERSION'
RELEASE_EXE_URL = f'https://github.com/{GITHUB_REPO}/releases/latest/download/SimplificaE.zip'

# Timeout em segundos para requests HTTP
HTTP_TIMEOUT = 10

# ---------------------------------------------------------------------------
# Estado compartilhado (thread-safe via lock)
# ---------------------------------------------------------------------------
_lock              = threading.Lock()
_update_available  = False
_remote_version    = ''
_check_done        = False
_download_progress = 0    # 0–100
_download_status   = ''   # '', 'downloading', 'ready', 'error'


def _read_local_version() -> str:
    """Lê VERSION do diretório do executável ou do script.
    Procura em: raiz do exe, _internal/, diretório do script.
    """
    candidates = []
    if getattr(sys, 'frozen', False):
        exe_dir = os.path.dirname(sys.executable)
        candidates.append(os.path.join(exe_dir, 'VERSION'))
        candidates.append(os.path.join(exe_dir, '_internal', 'VERSION'))
    # Diretório do próprio módulo updater.py
    candidates.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'VERSION'))
    for p in candidates:
        if os.path.exists(p):
            try:
                v = open(p).read().strip()
                if v:
                    return v
            except Exception:
                pass
    return '00000000_000'


def _fetch_remote_version() -> str:
    try:
        req = urllib.request.Request(VERSION_URL, headers={'Cache-Control': 'no-cache'})
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            return r.read().decode('utf-8').strip()
    except Exception:
        return ''


def _version_newer(remote: str, local: str) -> bool:
    """Compara versões no formato YYYYMMDD_NNN (string sort é suficiente)."""
    return remote > local


def check_update_async():
    """Inicia a verificação em thread daemon. Não bloqueia o startup."""
    t = threading.Thread(target=_check_worker, daemon=True)
    t.start()


def _md5_local_webapp() -> str:
    """MD5 do webapp.py local — mais confiável que VERSION para detectar updates."""
    candidates = []
    if getattr(sys, 'frozen', False):
        exe_dir = os.path.dirname(sys.executable)
        candidates.append(os.path.join(exe_dir, 'webapp.py'))
    candidates.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'webapp.py'))
    for p in candidates:
        if os.path.exists(p):
            try:
                import hashlib
                with open(p, 'rb') as f:
                    return hashlib.md5(f.read()).hexdigest()
            except Exception:
                pass
    return ''


def _fetch_remote_md5() -> str:
    """MD5 do webapp.py no GitHub."""
    try:
        import hashlib
        WEBAPP_URL = f'https://raw.githubusercontent.com/{GITHUB_REPO}/{GITHUB_BRANCH}/webapp.py'
        req = urllib.request.Request(WEBAPP_URL, headers={'Cache-Control': 'no-cache'})
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            return hashlib.md5(r.read()).hexdigest()
    except Exception:
        return ''


def _check_worker():
    global _update_available, _remote_version, _check_done
    try:
        local_version  = _read_local_version()
        remote_version = _fetch_remote_version()

        # Comparar por MD5 do webapp.py — mais confiável que VERSION
        # (o launcher atualiza o webapp.py mas pode não atualizar o VERSION local)
        local_md5  = _md5_local_webapp()
        remote_md5 = _fetch_remote_md5() if local_md5 else ''

        # Há update se VERSION é diferente OU se MD5 do webapp é diferente
        has_update = False
        if remote_version and _version_newer(remote_version, local_version):
            has_update = True
        elif local_md5 and remote_md5 and local_md5 != remote_md5:
            has_update = True

        with _lock:
            _remote_version   = remote_version
            _update_available = has_update
            _check_done       = True
    except Exception:
        with _lock:
            _check_done = True


def get_status() -> dict:
    """Retorna estado atual para o endpoint /check_update."""
    with _lock:
        return {
            'check_done':        _check_done,
            'has_update':        _update_available,
            'remote_version':    _remote_version,
            'local_version':     _read_local_version(),
            'download_progress': _download_progress,
            'download_status':   _download_status,
        }


# ---------------------------------------------------------------------------
# Download + substituição do .exe
# ---------------------------------------------------------------------------

def _exe_dir() -> str:
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


def _create_update_bat(new_exe: str, old_exe: str) -> str:
    """
    Cria update.bat que:
      - Espera o processo atual terminar (pelo PID)
      - Copia o novo .exe sobre o antigo
      - Relança o app
      - Deleta a si mesmo
    """
    bat_path = os.path.join(_exe_dir(), 'update.bat')
    pid      = os.getpid()
    script   = f"""@echo off
:wait
tasklist /FI "PID eq {pid}" 2>NUL | find /I "{pid}" >NUL
if not errorlevel 1 (
    timeout /T 1 /NOBREAK >NUL
    goto wait
)
copy /Y "{new_exe}" "{old_exe}"
start "" "{old_exe}"
del "%~f0"
del "{new_exe}"
"""
    with open(bat_path, 'w') as f:
        f.write(script)
    return bat_path


def start_download_and_update():
    """Inicia download em background. Chame a partir do endpoint /do_update."""
    t = threading.Thread(target=_download_worker, daemon=True)
    t.start()


def _download_worker():
    global _download_progress, _download_status

    with _lock:
        _download_status   = 'downloading'
        _download_progress = 0

    try:
        exe_dir  = _exe_dir()
        new_exe  = os.path.join(exe_dir, 'SimplificaE_new.exe')
        old_exe  = os.path.join(exe_dir, 'SimplificaE.exe') if getattr(sys, 'frozen', False) else None

        # Download com progresso
        def _reporthook(count, block_size, total_size):
            global _download_progress
            if total_size > 0:
                pct = min(int(count * block_size * 100 / total_size), 99)
                with _lock:
                    _download_progress = pct

        import zipfile
        from io import BytesIO

        # Para zip: baixa tudo, extrai .exe
        data_bytes = b''
        req = urllib.request.Request(RELEASE_EXE_URL)
        with urllib.request.urlopen(req, timeout=120) as r:
            total = int(r.headers.get('Content-Length', 0))
            downloaded = 0
            chunks = []
            while True:
                chunk = r.read(65536)
                if not chunk:
                    break
                chunks.append(chunk)
                downloaded += len(chunk)
                if total > 0:
                    with _lock:
                        _download_progress = min(int(downloaded * 99 / total), 99)
            data_bytes = b''.join(chunks)

        # Extrai SimplificaE.exe do zip
        with zipfile.ZipFile(BytesIO(data_bytes)) as zf:
            exe_entries = [n for n in zf.namelist() if n.lower().endswith('simplificae.exe')]
            if not exe_entries:
                raise RuntimeError('SimplificaE.exe não encontrado no ZIP')
            with zf.open(exe_entries[0]) as src, open(new_exe, 'wb') as dst:
                dst.write(src.read())

        with _lock:
            _download_progress = 100
            _download_status   = 'ready'

        # Se rodando como .exe, cria bat e dispara. Senão, apenas avisa.
        if old_exe and os.path.exists(old_exe):
            bat = _create_update_bat(new_exe, old_exe)
            subprocess.Popen(['cmd.exe', '/c', bat], creationflags=0x00000008)  # DETACHED_PROCESS

    except Exception as ex:
        with _lock:
            _download_status   = 'error'
            _download_progress = 0
