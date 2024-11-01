package main

import (
    "log"
    "math/rand"
    "os"
    "time"

    "github.com/gocql/gocql"
)

func main() {
    // Configuración de variables de entorno
    cassandraHost := os.Getenv("CASSANDRA_SEEDS")
    topic := os.Getenv("TOPIC")
    consistency := os.Getenv("CONSISTENCY")

    // Configuración de la conexión al clúster de Cassandra
    cluster := gocql.NewCluster(cassandraHost)
    cluster.Consistency = gocql.ParseConsistency(consistency)
    session, err := cluster.CreateSession()
    if err != nil {
        log.Fatalf("Error al conectar con Cassandra: %v", err)
    }
    defer session.Close()

    log.Printf("Conectado al clúster en %s con consistencia %s para el tópico %s", cassandraHost, consistency, topic)

    // Crear tabla de datos principal si no existe
    if err := session.Query(`
        CREATE TABLE IF NOT EXISTS ece573.prj04 (
            topic text,
            seq int,
            value float,
            PRIMARY KEY (topic, seq)
        )`).Exec(); err != nil {
        log.Fatalf("Error al crear la tabla ece573.prj04: %v", err)
    }

    log.Println("Tabla ece573.prj04 lista.")

    // Iniciar la inserción secuencial de datos desde seq=1
    for seq := 1; ; seq++ {
        value := rand.Float64()
        err := session.Query(
            `INSERT INTO ece573.prj04 (topic, seq, value) VALUES (?, ?, ?)`,
            topic, seq, value).Exec()
        if err != nil {
            log.Printf("No se pudo escribir el valor %d en la tabla ece573.prj04: %v", seq, err)
            time.Sleep(10 * time.Second) // Esperar 10 segundos antes de reintentar
            seq-- // Retroceder el contador de secuencia
            continue
        }
        if seq%1000 == 0 {
            log.Printf("%s: inserted %d rows", topic, seq)
        }
    }
}
