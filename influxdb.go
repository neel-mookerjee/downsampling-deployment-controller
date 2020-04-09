package main

import (
	"github.com/influxdata/influxdb/client/v2"
	log "github.com/sirupsen/logrus"
	"time"
)

type InfluxdbInterface interface {
	CreateDatabaseAndRp(db string, rp string) ([]client.Result, error)
}

type Influxdb struct {
	Url            string
	User           string
	Password       string
	InfluxdbClient client.Client
}

func NewInfluxdb(url string, user string, password string) (*Influxdb, error) {
	i := Influxdb{Url: url, User: user, Password: password}
	clnt, err := i.getClient()
	if err != nil {
		return &i, err
	}

	i.InfluxdbClient = clnt
	i.checkUpStatus()
	return &i, err
}

func (i *Influxdb) getClient() (client.Client, error) {
	// Create a new influxdb HTTPClient
	return client.NewHTTPClient(client.HTTPConfig{
		Addr:     i.Url,
		Username: i.User,
		Password: i.Password,
	})
}

func (i *Influxdb) checkUpStatus() {
	for {
		log.Printf("Checking if InfluxDB is up at %s", i.Url)
		d, v, err := i.InfluxdbClient.Ping(10 * time.Second)
		if err == nil {
			return
		} else {
			log.Printf("Got Response: %v, %s, %v Retrying after 5s...", d, v, err)
			time.Sleep(5 * time.Second)
		}
	}
}

func (i *Influxdb) CreateDatabaseAndRp(db string, rp string) error {
	cmd := "CREATE DATABASE \"" + db + "\" WITH DURATION 0s REPLICATION 1 SHARD DURATION 0s NAME \"" + rp + "\""
	log.Printf("Running %s", cmd)
	res, err := i.query(cmd)
	log.Printf("Response: %v", res)
	return err
}

func (i *Influxdb) query(cmd string) (res *client.Response, err error) {
	q := client.Query{
		Command: cmd,
	}

	response, err := i.InfluxdbClient.Query(q)
	if err == nil {
		if response.Error() != nil {
			return response, response.Error()
		}
	} else {
		return response, err
	}

	return response, nil
}
