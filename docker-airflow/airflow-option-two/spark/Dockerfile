# Usa la imagen base de Spark que ya estás utilizando
FROM bde2020/spark-worker:3.3.0-hadoop3.3

# Instala librerías y dependencias
USER root
RUN apk update && \
    apk add --no-cache \
    # Herramientas de compilación necesarias para construir librerías de Python con código C/C++
    build-base \
    # Necesario para PySpark y librerías que usan SSL
    openssl-dev \
    # Headers de Python 3 (Crucial para encontrar Python.h)
    python3-dev \
    # Instala las librerías binarias de librdkafka (versión de Alpine)
    # Dejamos estos paquetes instalados aunque no se usen directamente, para evitar problemas de compatibilidad futuros.
    librdkafka \
    librdkafka-dev \
    && rm -rf /var/cache/apk/*

# Añade explícitamente el usuario 'spark' si la imagen base lo perdió, 
# ya que la instrucción 'USER spark' falló. (UID/GID 1000 es el común en estos entornos Alpine)
RUN addgroup -g 1000 spark || true && \
    adduser -u 1000 -G spark -s /bin/sh -D spark || true

# NOTA CRÍTICA: Eliminamos la instalación de confluent-kafka. 
# La librería es incompatible con la versión de librdkafka que tiene Alpine en esta imagen base de Spark (3.3.0).
# La aplicación de PySpark usará el conector de Spark (vía --packages), que es más estable.

# Vuelve al usuario por defecto (spark)
USER spark
