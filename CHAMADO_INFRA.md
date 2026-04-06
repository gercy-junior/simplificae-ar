# CHAMADO — Time de Plataforma/Infra PicPay

## Título
Deploy de webapp Flask interno — SimplificaÊ (Antecipação de Recebíveis)

## Contexto
Desenvolvemos internamente um webapp Python/Flask para a mesa de operações
de Antecipação de Recebíveis PJ. Preciso de hospedagem interna homologada.

## O que a aplicação faz
- Operadores sobem arquivos CSV de agenda de recebíveis
- A aplicação consulta o Databricks para buscar seller_ids e elegibilidade
- Gera planilhas XLSX de cotação e seleção de URs
- Envia cotações por e-mail para os clientes
- 3-5 usuários simultâneos (mesa de AR interna)

## Stack
- Python 3.12 + Flask 3.1
- Containerizado (Dockerfile pronto)
- Repositório: https://github.com/gercy-junior/simplificae-ar

## Necessidades

### 1. Infraestrutura
- Container Docker (0.5-1 vCPU, 512MB-1GB RAM)
- Volume persistente: 10 GB (para uploads, outputs e histórico)
- Porta: 8080

### 2. Network
- Acesso de saída para: picpay-principal.cloud.databricks.com:443
- Acesso interno apenas (não precisa ser público na internet)

### 3. Service Account Databricks ⚠️ (crítico)
Preciso de um service principal com:
- Leitura em: picpay.sellers.all_vendors
- Leitura em: picpay.sellers.eligibility  
- Acesso ao SQL Warehouse: 6077a99f149e0d70 (Exploração 04)
- Token PAT permanente (não expira)

Atualmente usando OAuth pessoal (gercy.junior@picpay.com) que expira a cada 1h.

### 4. CI/CD
- Push na branch main → deploy automático
- Build via Dockerfile

### 5. URL
- Qualquer URL interna serve (ex: simplificae-ar.apps.picpay.com)

## Arquivos de referência
- Dockerfile e requirements.txt: https://github.com/gercy-junior/simplificae-ar
- Documento técnico completo: DEPLOY_REQUISITOS.md no repositório

## Contato
gercy.junior@picpay.com — Squad Crédito PJ / Antecipação de Recebíveis
