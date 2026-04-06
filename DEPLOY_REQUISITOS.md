# SimplificaÊ — Requisitos Técnicos para Deploy Interno
## Documento para time de Plataforma/Infra PicPay

---

## 1. Visão Geral

**Aplicação:** SimplificaÊ 🏭  
**Finalidade:** Webapp interno para cotação e seleção de URs (Unidades de Recebíveis) para antecipação de recebíveis PJ  
**Solicitante:** Gercy Junior (gercy.junior@picpay.com) — PM Crédito PJ / Antecipação de Recebíveis  
**Usuários:** 3-5 operadores da mesa de AR (uso interno, não exposição externa)  
**Repositório:** https://github.com/gercy-junior/simplificae-ar (pode migrar para GitHub Enterprise PicPay)

---

## 2. Stack Técnica

| Componente | Tecnologia | Versão |
|------------|-----------|--------|
| Linguagem | Python | 3.12 |
| Framework web | Flask | 3.1.3 |
| Servidor produção | Gunicorn | 23.0.0 |
| Geração de planilhas | openpyxl + xlsxwriter | 3.1.5 / 3.2.9 |
| Container | Docker | qualquer versão recente |

**Dependências completas:** `requirements.txt` no repositório

---

## 3. Integração com Databricks

A aplicação **depende do Databricks** para:

| Integração | Finalidade | Warehouse |
|-----------|-----------|-----------|
| `picpay.sellers.all_vendors` | Buscar seller_ids dos CNPJs | Exploração 04 |
| `picpay.sellers.eligibility` | Verificar elegibilidade dos sellers | Exploração 04 |

**Requisito:** Criar um **Service Principal / Service Account** no Databricks com:
- Acesso de leitura às tabelas `picpay.sellers.*`
- Acesso ao SQL Warehouse ID: `6077a99f149e0d70`
- Token PAT (Personal Access Token) de longa duração (não expira)

> ⚠️ Atualmente usa OAuth pessoal (gercy.junior@picpay.com) — token expira a cada 1h. Precisa substituir por service account para produção.

---

## 4. Variáveis de Ambiente Necessárias

```env
# Databricks — SERVICE ACCOUNT (não OAuth pessoal)
DATABRICKS_TOKEN=dapi_service_account_token_aqui
DATABRICKS_HOST=https://picpay-principal.cloud.databricks.com
DATABRICKS_WAREHOUSE_ID=6077a99f149e0d70

# Aplicação
PORT=8080
FLASK_SECRET_KEY=gere_com_secrets_token_hex_32

# SMTP para envio de cotações por email (opcional — pode usar gmail corporativo)
SMTP_USER=operador@picpay.com
SMTP_PASS=app_password_gmail
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
```

---

## 5. Requisitos de Infraestrutura

### Compute
| Recurso | Mínimo | Recomendado |
|---------|--------|-------------|
| CPU | 0.5 vCPU | 1 vCPU |
| RAM | 512 MB | 1 GB |
| Disco (volume persistente) | 5 GB | 10 GB |

> O app gera planilhas XLSX em disco temporariamente (100-900 KB por empresa por cotação). Os arquivos são limpos após download. O volume persistente armazena histórico de cotações e cache de sellers.

### Network
- **Saída (egress):** precisa acessar `picpay-principal.cloud.databricks.com:443`
- **Entrada:** apenas interna (intranet PicPay), sem exposição para internet
- **Porta:** 8080 (configurável via variável `PORT`)

### Storage (Volume Persistente)
A aplicação usa 3 diretórios que precisam sobreviver a restarts:

```
/app/uploads/     — arquivos CSV de agenda enviados pelos operadores (~100 MB)
/app/output/      — planilhas XLSX geradas (~500 MB)
/app/             — arquivos de configuração (email_config.json, raizes_conhecidas.json)
```

---

## 6. Dockerfile

```dockerfile
FROM python:3.12-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends gcc && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY webapp.py raizes_conhecidas.json ./
RUN mkdir -p /app/uploads /app/output
ENV PYTHONUNBUFFERED=1
EXPOSE 8080
CMD gunicorn --bind 0.0.0.0:${PORT:-8080} --workers 2 --timeout 120 webapp:app
```

---

## 7. Health Check

A aplicação expõe endpoint de health check:

```
GET /server_status
Resposta: {"is_server": true, "connected": false/true, "url": null}
HTTP 200
```

---

## 8. CI/CD

**Modelo desejado:** push na branch `main` → build automático → deploy

- Repositório atual: GitHub.com (`gercy-junior/simplificae-ar`)
- **Pode migrar para:** GitHub Enterprise PicPay se necessário
- Build: Dockerfile (sem dependências externas além do PyPI)
- Não usa nenhum registry externo

---

## 9. Autenticação de Usuários

**Situação atual:** sem autenticação (acesso aberto)  
**Situação desejada:** SSO PicPay (Google Workspace / Azure AD) na frente da aplicação, ou pelo menos autenticação básica por IP (apenas rede interna PicPay)

> Sugestão: proteção via VPN/rede interna é suficiente para o caso de uso (mesa de AR interna)

---

## 10. Histórico de Uso por Operador

A aplicação já registra internamente:
- Timestamp de cada cotação gerada
- Operador responsável (email)
- Empresa, valor, taxa utilizada
- Arquivo gerado

Endpoint: `GET /history` — retorna JSON com histórico completo  
Endpoint: `GET /api/history` — retorna JSON paginado

Esses dados ficam em arquivo `history.json` no volume persistente.

---

## 11. Resumo do Chamado para Infra

**Título:** Deploy de webapp Flask interno — SimplificaÊ AR  
**Tipo:** Novo serviço interno  
**Prioridade:** Média  

**O que preciso:**
1. Hospedagem do container Docker Python/Flask (specs acima)
2. Volume persistente de 10 GB montado em `/app/uploads` e `/app/output`
3. Acesso de rede sainte para `picpay-principal.cloud.databricks.com:443`
4. URL interna (ex: `simplificae-ar.apps.picpay.com` ou similar)
5. **Service account Databricks** com acesso às tabelas `picpay.sellers.*`
6. CI/CD a partir do repositório GitHub (push → deploy)

**Contato técnico:** gercy.junior@picpay.com  
**Squad:** Crédito PJ / Antecipação de Recebíveis

---

## 12. Alternativas Consideradas e Descartadas

| Plataforma | Motivo do descarte |
|-----------|-------------------|
| Railway.app | Não é plataforma homologada PicPay |
| Render.com | Não é plataforma homologada PicPay |
| Heroku | Não é plataforma homologada PicPay |

**Plataformas preferidas:** Moonlight PicPay, Kubernetes interno, AWS/GCP (se homologado pelo squad de infra)
