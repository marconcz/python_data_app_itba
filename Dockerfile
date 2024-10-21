# Usa una imagen base de Python
FROM apache/airflow:2.10.1

# Establece el directorio de trabajo
WORKDIR /usr/local/airflow

# Copia los archivos de tu proyecto al contenedor
COPY ./dags/ ./dags/
COPY ./cheapshark.py ./cheapshark.py
COPY ./entrypoint.sh .

# Instala las dependencias necesarias (puedes agregar otras según tu proyecto)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Exponer el puerto 8080 para acceder a la interfaz de usuario de Airflow
EXPOSE 8080

# Define el script de entrada
ENTRYPOINT ["./entrypoint.sh"]
