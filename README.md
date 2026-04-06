# SimplificaÊ 🏭 — Antecipação de Recebíveis PicPay

Webapp interno para cotação e seleção de URs (Unidades de Recebíveis) para antecipação.

## Stack
- Python 3.12 + Flask 3.1
- Databricks (consulta de sellers e elegibilidade)
- xlsxwriter / openpyxl (geração de planilhas)

## Deploy (Railway)

### 1. Fork/clone este repositório no GitHub

### 2. Criar projeto no Railway
1. Acesse [railway.app](https://railway.app)
2. **New Project → Deploy from GitHub repo**
3. Selecione este repositório
4. Railway detecta o Dockerfile automaticamente

### 3. Configurar variáveis de ambiente no Railway
No painel do Railway → **Variables**:

```
DATABRICKS_TOKEN=dapi_seu_service_account_token
DATABRICKS_HOST=https://picpay-principal.cloud.databricks.com
DATABRICKS_WAREHOUSE_ID=6077a99f149e0d70
FLASK_SECRET_KEY=gere_uma_chave_com_python_secrets
PORT=8080
```

### 4. Deploy automático
Cada push na branch `main` → Railway faz deploy automático.

## Desenvolvimento local

```bash
# Instalar dependências
pip install -r requirements.txt

# Configurar .env
cp .env.example .env
# Editar .env com suas credenciais

# Iniciar
python webapp.py
# ou com token OAuth local:
python _start_clean.py
```

## Estrutura
```
webapp.py          — Aplicação principal (Flask)
raizes_conhecidas.json — Mapeamento de raízes CNPJ → empresa
requirements.txt   — Dependências Python
Dockerfile         — Container para deploy
railway.toml       — Config Railway
.env.example       — Template de variáveis de ambiente
```

## Histórico de uso
O histórico de cotações por operador fica em `/history` na interface web.
Em produção, os arquivos são persistidos no volume do Railway.
