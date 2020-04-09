package main

import (
	"fmt"
	"testing"
)

func Test_DownsamplingObject_SortByTime(t *testing.T) {
	var dsList DownsampleObjects
	dsList = append(dsList, DownsamplingObject{QueryId: "q1", CreatedAt: "2017-12-08T21:46:28Z"})
	dsList = append(dsList, DownsamplingObject{QueryId: "q2", CreatedAt: "2017-12-08T21:46:27Z"})
	dsList = append(dsList, DownsamplingObject{QueryId: "q3", CreatedAt: "2018-12-08T21:46:27Z"})
	dsList = append(dsList, DownsamplingObject{QueryId: "q4", CreatedAt: "2017-11-08T21:46:27Z"})
	dsList = append(dsList, DownsamplingObject{QueryId: "q5", CreatedAt: "2017-12-04T21:46:27Z"})
	dsList = append(dsList, DownsamplingObject{QueryId: "q6", CreatedAt: "2012-12-04T22:46:27Z"})
	dsList = append(dsList, DownsamplingObject{QueryId: "q7", CreatedAt: "2017-12-08T20:46:27Z"})
	dsList = append(dsList, DownsamplingObject{QueryId: "q8", CreatedAt: "2017-12-04T21:00:27Z"})
	dsList = append(dsList, DownsamplingObject{QueryId: "q9", CreatedAt: "2017-12-04T21:50:27Z"})
	dsList.SortByTime()
	if dsList[0].QueryId != "q6" {
		t.Error(fmt.Sprintf("%s expected to be %s but found %s", "QueryId", "q1", dsList[0].QueryId))
	}
	if dsList[1].QueryId != "q4" {
		t.Error(fmt.Sprintf("%s expected to be %s but found %s", "QueryId", "q4", dsList[1].QueryId))
	}
	if dsList[2].QueryId != "q8" {
		t.Error(fmt.Sprintf("%s expected to be %s but found %s", "QueryId", "q8", dsList[2].QueryId))
	}
}
