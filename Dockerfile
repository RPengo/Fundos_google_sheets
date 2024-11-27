# Use a imagem base do Pythonk
FROM python:3.9-slim

# Defina o diretório de trabalho
WORKDIR /app

# Copie os arquivos necessários para o contêiner
COPY requirements.txt requirements.txt
COPY many.py many.py

# Instale as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Defina a variável de ambiente para a Flask
ENV FLASK_APP=many.py

# Exponha a porta em que a aplicação Flask estará rodando
EXPOSE 8080

# Comando para rodar a aplicação Flask
CMD ["flask", "run", "--host=0.0.0.0", "--port=8080"]
