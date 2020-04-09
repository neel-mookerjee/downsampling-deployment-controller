package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

type InfluxDbTestSuite struct {
}

func NewInfluxDbTestSuite() *InfluxDbTestSuite {
	return &InfluxDbTestSuite{}
}

type MockInfluxDbServerResponse struct {
	ResponseCode   int
	ResponseString string
}

func (tc *MockInfluxDbServerResponse) influxHttpHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(tc.ResponseCode)
	w.Write([]byte(tc.ResponseString))
}

func Test_Influxdb_CreateDatabaseAndRp(t *testing.T) {
	influxSpy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		qs := r.URL.Query()["q"]
		if qs[0] == "CREATE DATABASE \"db\" WITH DURATION 0s REPLICATION 1 SHARD DURATION 0s NAME \"rp\"" {
			w.Header().Add("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(200)
			w.Write([]byte("{\"status\":\"success\"}"))
		} else {
			w.WriteHeader(500)
		}
	}))
	defer influxSpy.Close()
	i := Influxdb{Url: influxSpy.URL, User: "", Password: ""}
	clnt, err := i.getClient()
	i.InfluxdbClient = clnt

	err = i.CreateDatabaseAndRp("db", "rp")
	if err != nil {
		t.Error(fmt.Sprintf("Was not expecting error but received. %v", err))
	}
}

func Test_Influxdb_CreateDatabaseAndRp_Error(t *testing.T) {
	influxSpy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer influxSpy.Close()
	i := Influxdb{Url: influxSpy.URL, User: "", Password: ""}
	clnt, err := i.getClient()
	i.InfluxdbClient = clnt

	err = i.CreateDatabaseAndRp("db", "rp")
	if err == nil {
		t.Error(fmt.Sprintf("Was expecting error but not received"))
	}
}
