# monitoreo-sensores-kafka-spark
Monitoreo de temperatura y humedad con datos simulados mediante Python, Apache Kafka y Spark Structured Streaming.
# Monitoreo de sensores con Kafka y Spark

## Descripción

Práctica de procesamiento continuo de datos simulados de temperatura y humedad. Un productor en Python envía lecturas a Apache Kafka y Spark Structured Streaming calcula los promedios por sensor en ventanas de un minuto.

Los datos son simulados; no provienen de sensores físicos.

## Archivos

- `kafka_producer.py`: genera una lectura cada segundo y la envía al tema `sensor_data`.
- `spark_streaming_consumer.py`: recibe los registros de Kafka y calcula los promedios de temperatura y humedad.

## Entorno utilizado

- Ubuntu 22.04 en VirtualBox.
- Java 11.
- Python 3.10.
- Apache Kafka 3.9.2 con ZooKeeper.
- Apache Spark 3.5.3, compilado para Scala 2.12.
- Biblioteca kafka-python 3.0.11.
- PuTTY para acceder a la máquina virtual mediante SSH.

## Requisitos previos

Estas instrucciones suponen que Kafka y Spark ya están instalados y configurados para el usuario `hadoop` en las siguientes rutas:

- Kafka: `/home/hadoop/kafka`
- Spark: `/home/hadoop/spark`
- Java: `/usr/lib/jvm/java-11-openjdk-amd64`

Los dos archivos Python deben estar en `/home/hadoop`.

Si se utiliza otro usuario o ubicación, se deben ajustar las rutas de los comandos y la ruta `checkpointLocation` del consumidor.

## Datos generados

Cada registro contiene:

- `sensor_id`: identificador entre 1 y 10.
- `temperature`: temperatura simulada entre 20 y 30.
- `humidity`: humedad simulada entre 30 y 70.
- `timestamp`: marca de tiempo Unix en segundos.

## Ejecución

Ejecutar los comandos dentro de Ubuntu con el usuario `hadoop`.

### 1. Configurar Java e instalar la biblioteca del productor

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
python3 -m pip install --user kafka-python==3.0.11
```

### 2. Iniciar ZooKeeper

Si el servicio ya está activo, no iniciarlo nuevamente.

```bash
~/kafka/bin/zookeeper-server-start.sh -daemon ~/kafka/config/zookeeper.properties
```

### 3. Iniciar Kafka

Esperar a que ZooKeeper esté disponible. Si Kafka ya está activo, no iniciarlo nuevamente.

```bash
~/kafka/bin/kafka-server-start.sh -daemon ~/kafka/config/server.properties
```

Comprobar los procesos:

```bash
jps
```

Deben aparecer `QuorumPeerMain` y `Kafka`.

### 4. Crear el tema

Con Kafka disponible, ejecutar:

```bash
~/kafka/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sensor_data
```

Verificar el tema:

```bash
~/kafka/bin/kafka-topics.sh --describe --bootstrap-server localhost:9092 --topic sensor_data
```

### 5. Ejecutar el productor

En una primera terminal:

```bash
python3 ~/kafka_producer.py
```

La consola mostrará mensajes `Sent:` con las lecturas enviadas. Mantener esta terminal abierta.

### 6. Ejecutar el consumidor

En una segunda terminal, con el mismo usuario:

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
~/spark/bin/spark-submit --master 'local[2]' --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 ~/spark_streaming_consumer.py
```

La primera ejecución necesita conexión a Internet para descargar el conector de Kafka y sus dependencias.

## Resultados esperados

Spark muestra una tabla con estas columnas:

- `window`: intervalo de un minuto.
- `sensor_id`: identificador del sensor.
- `temperatura_promedio`: promedio de temperatura.
- `humedad_promedio`: promedio de humedad.

La consulta intenta procesar un lote cada 10 segundos. El número `Batch` aumenta mientras el programa continúa activo.

En el primer inicio se reciben los registros nuevos a partir del arranque del consumidor. Al reiniciar con el mismo checkpoint, se retoma el progreso guardado.

El modo `complete` muestra los resultados acumulados y la consola presenta hasta 50 filas por lote. Esta configuración corresponde a una demostración académica.

## Interfaz web de Spark

Mientras el consumidor se ejecuta, la interfaz utiliza normalmente el puerto 4040.

Para acceder desde Windows mediante PuTTY:

1. Abrir `Change Settings`.
2. Ir a `Connection > SSH > Tunnels`.
3. Escribir `4040` en `Source port`.
4. Escribir `localhost:4040` en `Destination`.
5. Seleccionar `Local`, pulsar `Add` y después `Apply`.
6. Abrir http://127.0.0.1:4040 en el navegador de Windows.

Se pueden consultar las pestañas Jobs, Stages, Environment, Executors y Structured Streaming.

Esta dirección es local y requiere que la sesión SSH y la aplicación permanezcan activas.

## Detener la demostración

Presionar `Ctrl + C` en la terminal del productor y en la del consumidor.

## Alcance

Este repositorio contiene el componente de procesamiento en tiempo real de la práctica. Los registros son simulados y los resultados se presentan en consola.
