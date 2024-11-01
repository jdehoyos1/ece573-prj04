package main

import (
	"log"
	"math/rand"
	"os"
	"strings"

	"github.com/gocql/gocql"
)

func main() {
	topic := os.Getenv("TOPIC")
	if topic == "" {
		log.Fatalf("Unknown topic")
	}

	cs := os.Getenv("CONSISTENCY")
	consistency := gocql.All
	switch strings.ToUpper(cs) {
	case "ALL":
	case "ONE":
		consistency = gocql.One
	case "QUORUM":
		consistency = gocql.Quorum
	default:
		log.Fatalf("Unknown consistency level %s", cs)
	}

	seed := os.Getenv("CASSANDRA_SEEDS")
	log.Printf(
		"Connecting cluster at %s with consistency %s for topic %s",
		seed, consistency, topic)

	cluster := gocql.NewCluster(seed)
	cluster.Consistency = consistency
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Cannot connect to cluster at %s: %v", seed, err)
	}
	defer session.Close()

	var clusterName string
	if err := session.Query(
		"SELECT cluster_name FROM system.local").
		Scan(&clusterName); err != nil {
		log.Fatalf("Cannot query cluster: %v", err)
	}
	log.Printf("Connected to cluster %s", clusterName)

	if err := session.Query(
		`CREATE KEYSPACE IF NOT EXISTS ece573
			WITH replication = {
				'class':'SimpleStrategy',
				'replication_factor':3}`).
		Exec(); err != nil {
		log.Fatalf("Cannot create keyspace ece573: %v", err)
	}

	if err := session.Query(
		`CREATE TABLE IF NOT EXISTS ece573.prj04 (
			topic text, seq int, value double,
			PRIMARY KEY (topic, seq))`).
		Exec(); err != nil {
		log.Fatalf("Cannot create table ece573.prj04: %v", err)
	}

	if err := session.Query(
		`CREATE TABLE IF NOT EXISTS ece573.prj04_last_seq (
			topic text, seq int,
			PRIMARY KEY (topic))`).
		Exec(); err != nil {
		log.Fatalf("Cannot create table ece573.prj04_last_seq: %v", err)
	}

	log.Printf("Tables ece573.prj04 and ece573.prj04_last_seq ready.")

	// Modify code below to read lastSeq from ece573.prj04_last_seq
	// Leer el último valor de seq desde ece573.prj04_last_seq
// Leer el último valor de seq desde ece573.prj04_last_seq
var lastSeq int
if err := session.Query(
    `SELECT seq FROM ece573.prj04_last_seq WHERE topic = ?`,
    topic).Scan(&lastSeq); err != nil {
    if err == gocql.ErrNotFound {
        lastSeq = 0
        log.Printf("No previous sequence found for topic %s, starting from lastSeq=0", topic)
    } else {
        log.Fatalf("Cannot read lastSeq from ece573.prj04_last_seq: %v", err)
    }
} else {
    log.Printf("Resuming from lastSeq=%d for topic %s", lastSeq, topic)
}




	log.Printf("%s: start from lastSeq=%d", topic, lastSeq)
	for seq := lastSeq + 1; ; seq++ {
		value := rand.Float64()
		err := session.Query(
			`INSERT INTO ece573.prj04 (topic, seq, value) VALUES (?, ?, ?)`,
			topic, seq, value).
			Exec()
		if err != nil {
			log.Fatalf("Cannot write %d to table ece573.prj04: %v", seq, err)
		}
		err = session.Query(
			`INSERT INTO ece573.prj04_last_seq (topic, seq) VALUES (?, ?)`,
			topic, seq).
			Exec()
		if err != nil {
			log.Fatalf("Cannot write %d to table ece573.prj04_last_seq: %v", seq, err)
		}

		if seq%1000 == 0 {
			log.Printf("%s: inserted rows to seq %d", topic, seq)
		}
	}
}