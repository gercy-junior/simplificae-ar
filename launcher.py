#!/usr/bin/env python3
"""
SimplificaE - Launcher v22 com Auto-Update via GitHub
Ao abrir, verifica se existe versao mais nova no GitHub e atualiza automaticamente.
"""
import os, sys, shutil, webbrowser, threading, socket, time, subprocess, json, hashlib

if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
    INTERNAL_DIR = os.path.join(BASE_DIR, '_internal')
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    INTERNAL_DIR = BASE_DIR

os.chdir(BASE_DIR)
os.environ['PYTHONIOENCODING'] = 'utf-8'

DATABRICKS_HOST = "https://picpay-principal.cloud.databricks.com"
ENV_FILE = os.path.join(INTERNAL_DIR, '.env')
SEP = '=' * 58

# URL raw do GitHub para o webapp.py
GITHUB_RAW_URL = "https://raw.githubusercontent.com/gercy-junior/simplificae-ar/main/webapp.py"
# URL da versao (arquivo de versao simples com numero)
GITHUB_VERSION_URL = "https://raw.githubusercontent.com/gercy-junior/simplificae-ar/main/VERSION"

# -------------------------------------------------------
# Carregar .env
# -------------------------------------------------------
def load_env():
    for env_dir in [BASE_DIR, INTERNAL_DIR]:
        ep = os.path.join(env_dir, '.env')
        if os.path.exists(ep):
            with open(ep, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        k, v = line.split('=', 1)
                        os.environ.setdefault(k.strip(), v.strip())
            return ep
    return None

# -------------------------------------------------------
# Auto-Update
# -------------------------------------------------------
def get_webapp_hash(path):
    """Retorna MD5 do webapp.py local."""
    try:
        with open(path, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()
    except Exception:
        return ''

def verificar_e_atualizar():
    """
    Verifica se existe nova versao no GitHub e atualiza se necessario.
    Retorna: 'atualizado', 'ok', 'sem_internet', 'erro'
    """
    try:
        import urllib.request, ssl
        
        # Contexto SSL que ignora certificados (rede corporativa PicPay)
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        
        webapp_local = os.path.join(BASE_DIR, 'webapp.py')
        if not os.path.exists(webapp_local):
            webapp_local = os.path.join(INTERNAL_DIR, 'webapp.py')
        
        hash_local = get_webapp_hash(webapp_local)
        
        # Baixar webapp.py do GitHub
        print('  Verificando atualizacoes...')
        req = urllib.request.Request(
            GITHUB_RAW_URL,
            headers={'User-Agent': 'SimplificaE-AutoUpdate/1.0', 'Cache-Control': 'no-cache'}
        )
        with urllib.request.urlopen(req, context=ctx, timeout=10) as resp:
            novo_conteudo = resp.read()
        
        hash_novo = hashlib.md5(novo_conteudo).hexdigest()
        
        if hash_novo == hash_local:
            print('  Versao atual. Nenhuma atualizacao necessaria.')
            return 'ok'
        
        # Tem atualizacao! Baixar e aplicar
        print()
        print('  NOVA VERSAO DISPONIVEL! Atualizando...')
        
        # Salvar backup do webapp atual
        bk = webapp_local + '.bak_autoupdate'
        shutil.copy2(webapp_local, bk)
        
        # Salvar novo webapp.py na raiz E no _internal
        with open(webapp_local, 'wb') as f:
            f.write(novo_conteudo)
        # Garantir que _internal também fica atualizado
        webapp_internal = os.path.join(INTERNAL_DIR, 'webapp.py')
        if webapp_internal != webapp_local and os.path.exists(INTERNAL_DIR):
            try:
                with open(webapp_internal, 'wb') as f:
                    f.write(novo_conteudo)
            except Exception:
                pass

        # Atualizar também os módulos auxiliares (updater.py, usage_log.py)
        # Isso garante que operadores sempre tenham a versão mais recente de tudo
        GITHUB_BASE = 'https://raw.githubusercontent.com/gercy-junior/simplificae-ar/main/'
        for modulo in ['updater.py', 'usage_log.py']:
            try:
                req_m = urllib.request.Request(
                    GITHUB_BASE + modulo,
                    headers={'User-Agent': 'SimplificaE-AutoUpdate/1.0', 'Cache-Control': 'no-cache'}
                )
                with urllib.request.urlopen(req_m, context=ctx, timeout=10) as rm:
                    conteudo = rm.read()
                # Salvar na raiz e no _internal
                for dest_dir in [BASE_DIR, INTERNAL_DIR]:
                    dest = os.path.join(dest_dir, modulo)
                    try:
                        with open(dest, 'wb') as fd:
                            fd.write(conteudo)
                    except Exception:
                        pass
            except Exception:
                pass  # falha silenciosa — não bloqueia o update principal

        # Atualizar VERSION em todos os lugares
        try:
            req_v = urllib.request.Request(GITHUB_VERSION_URL, headers={'User-Agent': 'SimplificaE-AutoUpdate/1.0'})
            with urllib.request.urlopen(req_v, context=ctx, timeout=5) as rv:
                nova_version = rv.read().decode('utf-8').strip()
            for vpath in [
                os.path.join(BASE_DIR, 'VERSION'),
                os.path.join(INTERNAL_DIR, 'VERSION'),
            ]:
                try:
                    with open(vpath, 'w') as vf:
                        vf.write(nova_version + '\n')
                except Exception:
                    pass
        except Exception:
            pass

        print('  Atualizado com sucesso!')
        return 'atualizado'
    
    except Exception as e:
        err = str(e).lower()
        if 'ssl' in err or 'certificate' in err or 'connect' in err or 'timeout' in err:
            return 'sem_internet'
        return 'erro'

# -------------------------------------------------------
# Databricks
# -------------------------------------------------------
def find_cli():
    """Encontra o Databricks CLI em qualquer instalação possível."""
    candidates = []

    # 1. WinGet — varre toda a pasta de packages sem depender do nome exato
    winget_base = os.path.join(os.environ.get('LOCALAPPDATA', ''), 'Microsoft', 'WinGet', 'Packages')
    if os.path.exists(winget_base):
        for folder in os.listdir(winget_base):
            if 'Databricks' in folder or 'databricks' in folder:
                c = os.path.join(winget_base, folder, 'databricks.exe')
                if os.path.exists(c):
                    candidates.append(c)

    # 2. PATH do sistema
    found = shutil.which('databricks')
    if found:
        candidates.append(found)

    # 3. Outros locais comuns
    extras = [
        os.path.join(os.environ.get('LOCALAPPDATA', ''), 'Programs', 'databricks', 'databricks.exe'),
        os.path.join(os.environ.get('PROGRAMFILES', ''), 'Databricks CLI', 'databricks.exe'),
        os.path.join(os.environ.get('USERPROFILE', ''), '.databricks', 'bin', 'databricks.exe'),
    ]
    candidates += [c for c in extras if os.path.exists(c)]

    for c in candidates:
        try:
            r = subprocess.run([c, 'version'], capture_output=True, timeout=5)
            if r.returncode == 0:
                return c
        except Exception:
            continue
    return None

def _get_configured_profiles():
    """Lê os profiles configurados no .databrickscfg do usuário."""
    cfg_path = os.path.join(os.environ.get('USERPROFILE', ''), '.databrickscfg')
    profiles = []
    if not os.path.exists(cfg_path):
        return profiles
    try:
        import configparser
        cfg = configparser.ConfigParser()
        cfg.read(cfg_path, encoding='utf-8')
        # Profiles que apontam para o host PicPay
        for section in cfg.sections():
            host = cfg.get(section, 'host', fallback='')
            if 'picpay' in host or section == 'DEFAULT':
                profiles.append(section)
        # DEFAULT section separada
        if cfg.defaults().get('host', '') and not profiles:
            profiles.append(None)  # None = sem --profile
    except Exception:
        pass
    return profiles


def get_token_from_cli(cli):
    """Obtém token OAuth via CLI. Tenta profiles configurados automaticamente."""
    # Profiles para tentar, em ordem de preferência
    profiles_to_try = _get_configured_profiles()
    # Sempre tenta sem profile também (usa DEFAULT)
    profiles_to_try.append(None)
    # Remove duplicatas mantendo ordem
    seen = set()
    profiles_ordered = []
    for p in profiles_to_try:
        if p not in seen:
            seen.add(p)
            profiles_ordered.append(p)

    for profile in profiles_ordered:
        try:
            cmd = [cli, 'auth', 'token', '--host', DATABRICKS_HOST]
            if profile:
                cmd += ['--profile', profile]
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            if r.returncode == 0:
                data = json.loads(r.stdout)
                token = data.get('access_token', '')
                if token:
                    return token
        except Exception:
            continue
    return ''

def save_token(token):
    lines = []
    found = False
    if os.path.exists(ENV_FILE):
        with open(ENV_FILE, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                if line.startswith('DATABRICKS_TOKEN='):
                    lines.append(f'DATABRICKS_TOKEN={token}\n')
                    found = True
                else:
                    lines.append(line)
    if not found:
        lines.append(f'DATABRICKS_TOKEN={token}\n')
    with open(ENV_FILE, 'w', encoding='utf-8') as f:
        f.writelines(lines)
    os.environ['DATABRICKS_TOKEN'] = token

def token_valido():
    """Verifica se o token atual é válido.
    PAT dapi* são considerados inválidos sempre — usar CLI OAuth.
    """
    token = os.environ.get('DATABRICKS_TOKEN', '')
    if not token or len(token) < 30:
        return False
    # PAT expirado: nunca confiar, sempre renovar via CLI
    if token.startswith('dapi'):
        return False
    return True

def setup_databricks():
    if token_valido():
        return True
    
    cli = find_cli()
    if cli:
        print('  Renovando token Databricks automaticamente...')
        token = get_token_from_cli(cli)
        if token:
            save_token(token)
            print('  Token renovado!')
            return True
    
    print()
    print(SEP)
    print('  CONFIGURACAO DO DATABRICKS')
    print(SEP)
    print()
    print('  Nao foi possivel conectar ao Databricks PicPay.')
    print()
    print('  POSSIVEIS CAUSAS:')
    print('  - Primeiro acesso neste computador')
    print('  - Sessao expirada (ocorre apos ~90 dias)')
    print('  - VPN desconectada ou sem acesso a rede PicPay')
    print()
    print('  [1] Login automatico (recomendado)')
    print('      Abre o navegador > login com @picpay.com')
    print()
    print('  [2] Token manual')
    print('      Gere em: picpay-principal.cloud.databricks.com')
    print('      Settings > Developer > Access Tokens > Generate')
    print()
    print('  [3] Continuar sem Databricks')
    print('      Sellers nao serao buscados automaticamente')
    print()
    
    escolha = input('  Escolha (1, 2 ou 3): ').strip() or '1'
    
    if escolha == '3':
        return True
    elif escolha == '2':
        print()
        token = input('  Cole o token (dapi...): ').strip()
        if token and len(token) > 10:
            save_token(token)
            print('  Token salvo!')
            return True
        return False
    else:
        if not cli:
            print()
            print('  Instalando Databricks CLI...')
            try:
                subprocess.run(
                    ['winget', 'install', '--id', 'Databricks.DatabricksCLI',
                     '-e', '--source', 'winget',
                     '--accept-package-agreements', '--accept-source-agreements'],
                    timeout=300
                )
                time.sleep(2)
                cli = find_cli()
            except Exception as e:
                print(f'  Erro: {e}')
        
        if not cli:
            print()
            print('  Nao foi possivel instalar o CLI.')
            print('  Use opcao [2] ou contate: gercy.junior@picpay.com')
            input('  Enter para continuar...')
            return True
        
        print()
        print('  Abrindo navegador para login...')
        print('  Faca login com @picpay.com e volte aqui.')
        print()
        try:
            subprocess.run([cli, 'auth', 'login', '--host', DATABRICKS_HOST], timeout=300)
        except Exception:
            pass
        
        token = get_token_from_cli(cli)
        if token:
            save_token(token)
            print('  Login realizado! Token salvo.')
            return True
        return False

def renovar_background():
    cli = find_cli()
    if not cli:
        return
    def loop():
        while True:
            time.sleep(45 * 60)
            t = get_token_from_cli(cli)
            if t:
                save_token(t)
    threading.Thread(target=loop, daemon=True).start()

# -------------------------------------------------------
# Utilitarios
# -------------------------------------------------------
def kill_port(port):
    try:
        r = subprocess.run(['netstat', '-ano'], capture_output=True, text=True, timeout=10)
        pids = set()
        for line in r.stdout.splitlines():
            if f':{port} ' in line and ('LISTENING' in line or 'ESTABLISHED' in line):
                parts = line.strip().split()
                if parts:
                    try: pids.add(int(parts[-1]))
                    except: pass
        for pid in pids:
            try: subprocess.run(['taskkill', '/F', '/PID', str(pid)], capture_output=True, timeout=5)
            except: pass
        if pids:
            time.sleep(1)
    except: pass

def porta_livre(port, timeout=0.5):
    try:
        with socket.create_connection(('localhost', port), timeout=timeout):
            return False
    except OSError:
        return True

def open_browser():
    for _ in range(30):
        if not porta_livre(5000):
            break
        time.sleep(0.5)
    webbrowser.open('http://localhost:5000')

# -------------------------------------------------------
# MAIN
# -------------------------------------------------------
def main():
    print(SEP)
    print('  SimplificaE - Antecipacao de Recebiveis PicPay')
    print(SEP)
    print()

    # 1. Carregar .env
    load_env()

    # 2. Copiar arquivos na primeira execucao
    for fname in ['webapp.py', 'raizes_conhecidas.json']:
        dest = os.path.join(BASE_DIR, fname)
        src  = os.path.join(INTERNAL_DIR, fname)
        if not os.path.exists(dest) and os.path.exists(src):
            shutil.copy2(src, dest)

    # 3. Auto-update (silencioso, nao bloqueia se sem internet)
    status_update = verificar_e_atualizar()
    if status_update == 'atualizado':
        print()
        print('  App atualizado! Reiniciando...')
        time.sleep(1)
        # Reiniciar o processo com o novo webapp.py
        os.execv(sys.executable, [sys.executable] + sys.argv)
        return

    # 4. Setup Databricks
    setup_databricks()

    # 5. Renovacao de token em background
    renovar_background()

    # 6. Liberar porta
    if not porta_livre(5000):
        print('  Liberando porta 5000...')
        kill_port(5000)

    # 7. Info de uso
    print()
    print('  Acesse: http://localhost:5000')
    print('  (Feche esta janela para encerrar)')
    print()

    # 8. Abrir navegador
    threading.Thread(target=open_browser, daemon=True).start()

    # 9. Iniciar webapp
    sys.path.insert(0, BASE_DIR)
    from webapp import app
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\nEncerrado.')
        sys.exit(0)
    except Exception as e:
        print(f'\nErro: {e}')
        import traceback; traceback.print_exc()
        input('Enter para sair...')
        sys.exit(1)
