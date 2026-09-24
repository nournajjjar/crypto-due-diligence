FROM python:3.9-slim

ENV TF_CPP_MIN_LOG_LEVEL=2
ENV PYTHONIOENCODING=utf-8

WORKDIR /app

# Installer les dépendances système
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libxml2-dev \
    libxslt-dev \
    python3-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copier les fichiers de l'application
COPY . .

# Installer d'abord les packages légers
RUN pip install --no-cache-dir \
    flask \
    flask-wtf \
    requests \
    flask-socketio\
    eventlet\
    mysql-connector-python \
    pdfplumber \
    python-pptx \
    matplotlib \
    Plotly\
    yfinance

# Installer ensuite les packages data science classiques
RUN pip install --no-cache-dir \
    pandas \
    numpy \
    scikit-learn

# Installer en dernier tensorflow (le plus lourd et susceptible de planter)
RUN pip install --no-cache-dir \
    keras-tuner \
    tensorflow-cpu==2.15.0

EXPOSE 5000

CMD ["python", "app.py"]
