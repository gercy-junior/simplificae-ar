FROM python:3.12-slim

WORKDIR /app

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copiar e instalar dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar aplicação
COPY webapp.py .
COPY raizes_conhecidas.json .

# Criar diretórios de dados persistentes
RUN mkdir -p /app/uploads /app/output /app/skills

# Variáveis de ambiente padrão
ENV PYTHONUNBUFFERED=1
ENV PORT=8080

# Expor porta
EXPOSE 8080

# Iniciar com gunicorn (produção)
CMD gunicorn --bind 0.0.0.0:${PORT} --workers 2 --timeout 120 --access-logfile - --error-logfile - webapp:app
