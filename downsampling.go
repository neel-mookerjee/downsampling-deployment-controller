package main

import "sort"

type DownsamplingObject struct {
	Nickname          string `json:"nickname"`
	QueryId           string `json:"queryId"`
	QueryHash         string `json:"queryHash"`
	CreatedAt         string `json:"createdAt"`
	UpdatedAt         string `json:"updatedAt"`
	Db                string `json:"db"`
	Rp                string `json:"rp"`
	Measurement       string `json:"measurement"`
	TargetRp          string `json:"targetRp"`
	TargetMeasurement string `json:"targetMeasurement"`
	PreviewExpiresAt  string `json:"previewExpiresAt"`
	QueryState        string `json:"queryState"`
	Fields            []struct {
		Alias    string `json:"alias"`
		Field    string `json:"field"`
		Function string `json:"func"`
	} `json:"fields"`
	Tags                   []string `json:"tags"`
	Interval               int      `json:"interval"`
	IsHistoricDownsampling bool     `json:"isHistoricDownsampling"`
}

type DownsampleObjects []DownsamplingObject

func (slice DownsampleObjects) Len() int {
	return len(slice)
}

func (slice DownsampleObjects) Less(i, j int) bool {
	return slice[i].CreatedAt < slice[j].CreatedAt
}

func (slice DownsampleObjects) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

func (slice DownsampleObjects) SortByTime() {
	sort.Sort(slice)
}
