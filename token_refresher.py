#!/usr/bin/env python3
"""
Token refresher para produção.
Em Railway, o Databricks CLI não está disponível.
Este script usa o refresh_token OAuth para renovar o access_token automaticamente.

ALTERNATIVA RECOMENDADA: usar um PAT (Personal Access Token) do Databricks
que não expira. Solicite ao time de dados/infra do PicPay.
"""
import os
import json
import time
import requests
import threading
import logging

logger = logging.getLogger(__name__)

DATABRICKS_HOST = os.environ.get('DATABRICKS_HOST', 'https://picpay-principal.cloud.databricks.com')
TOKEN_REFRESH_URL = f"{DATABRICKS_HOST}/oidc/v1/token"

# Se tiver refresh_token configurado, usa; senao usa o access_token diretamente
REFRESH_TOKEN = os.environ.get('DATABRICKS_REFRESH_TOKEN', '')
CLIENT_ID = os.environ.get('DATABRICKS_CLIENT_ID', 'databricks-cli')


def refresh_access_token():
    """Tenta renovar o access_token usando o refresh_token."""
    if not REFRESH_TOKEN:
        return None
    try:
        resp = requests.post(TOKEN_REFRESH_URL, data={
            'grant_type': 'refresh_token',
            'refresh_token': REFRESH_TOKEN,
            'client_id': CLIENT_ID,
        }, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            new_token = data.get('access_token', '')
            if new_token:
                os.environ['DATABRICKS_TOKEN'] = new_token
                logger.info('Token Databricks renovado com sucesso')
                return new_token
    except Exception as e:
        logger.warning(f'Falha ao renovar token: {e}')
    return None


def start_token_refresh_scheduler():
    """Inicia thread de renovação automática a cada 45 minutos."""
    if not REFRESH_TOKEN:
        logger.info('DATABRICKS_REFRESH_TOKEN nao configurado — sem renovacao automatica')
        return

    def refresh_loop():
        while True:
            time.sleep(45 * 60)  # 45 minutos
            refresh_access_token()

    t = threading.Thread(target=refresh_loop, daemon=True)
    t.start()
    logger.info('Token refresh scheduler iniciado (intervalo: 45min)')
