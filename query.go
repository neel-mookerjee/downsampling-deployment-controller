package main

import (
	"errors"
	log "github.com/sirupsen/logrus"
	"time"
)

type DownsamplingItemHandlerInterface interface {
	GetAllDownsamplingItems() ([]DownsamplingObject, error)
	GetDownsamplingItem(queryId string) (DownsamplingObject, error)
	GetNextPendingDownsamplingItem() (DownsamplingObject, error)
	GetDeletedDownsamplingItems() ([]DownsamplingObject, error)
	DeployDownsamplingPendingSimulationItem(id string) error
	DeployDownsamplingItem(query DownsamplingObject) error
	DeleteDownsamplingItem(query DownsamplingObject) error
	HandleExpiredSimulations() (int, error)
}

type DownsamplingItemHandler struct {
	db      DbInterface
	config  *Config
	Metrics *Metrics
}

func NewDownsamplingItemHandler(config *Config, metrics *Metrics) (*DownsamplingItemHandler, error) {
	db, err := NewDynamodb(config)
	if err != nil {
		return nil, err
	}
	return &DownsamplingItemHandler{config: config, db: db, Metrics: metrics}, nil
}

func (u *DownsamplingItemHandler) GetDownsamplingItem(queryId string) (DownsamplingObject, error) {
	return u.db.GetDownsamplingItem(queryId)
}

func (u *DownsamplingItemHandler) GetNextPendingDownsamplingItem() (DownsamplingObject, error) {
	var ds DownsamplingObject
	dsList, err := u.db.GetPendingDownsampleItems()
	if err != nil {
		return ds, err
	}

	DownsampleObjects(dsList).SortByTime()

	if len(dsList) > 0 {
		ds = dsList[0]
	}

	return ds, err
}

func (u *DownsamplingItemHandler) GetAllDownsamplingItems() ([]DownsamplingObject, error) {
	log.Println("Getting all pending and preview pending items...")
	dsList, err := u.db.GetAllToDoItems()

	return dsList, err
}

func (u *DownsamplingItemHandler) GetDeletedDownsamplingItems() ([]DownsamplingObject, error) {
	log.Println("Getting old deleted items...")
	dsList, err := u.db.GetDeletedDownsampleItems()

	return dsList, err
}

func (u *DownsamplingItemHandler) DeployDownsamplingPendingSimulationItem(id string) error {
	var ds DownsamplingObject
	ds, err := u.db.GetDownsamplingItem(id)
	if err != nil {
		return err
	}

	if ds.QueryId == "" {
		log.Printf("Object for queryId %s not found, ignoring...", ds.QueryId)
		return nil
	} else if ds.QueryState != "PREVIEW_PENDING" {
		log.Printf("Object was supposed to have status PREVIEW_PENDING, but found it changed to %s, ignoring...", ds.QueryState)
		return nil
	}

	ds.QueryState = "PREVIEW_DEPLOYED"
	ds.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	ds.PreviewExpiresAt = time.Now().Add(time.Duration(u.config.ExpireAfterMinute) * time.Minute).Format(time.RFC3339)

	_, err = u.db.UpdateDownsamplingItem(ds)
	return err
}

func (u *DownsamplingItemHandler) DeployDownsamplingItem(query DownsamplingObject) error {
	var ds DownsamplingObject
	ds, err := u.db.GetDownsamplingItem(query.QueryId)
	if err != nil {
		return err
	}

	if ds.QueryId == "" {
		return errors.New("object not found")
	} else if ds.QueryState != "PENDING" {
		return errors.New("status changed")
	}

	ds.QueryState = "DEPLOYED"
	ds.UpdatedAt = time.Now().UTC().Format(time.RFC3339)

	_, err = u.db.UpdateDownsamplingItem(ds)
	return err
}

func (u *DownsamplingItemHandler) DeleteDownsamplingItem(query DownsamplingObject) error {
	return u.db.DeleteDownsamplingItem(query.QueryId)
}

func (d *DownsamplingItemHandler) HandleExpiredSimulations() (int, error) {
	count := 0
	dsList, err := d.GetExpiredItems()
	if err != nil {
		return count, err
	}

	for _, ds := range dsList {
		log.Printf("Deleting preview for queryId: %s", ds.QueryId)
		err := d.db.DeleteDownsamplingItem(ds.QueryId)
		if err != nil {
			return count, err
		}
		count++
	}

	return count, nil
}

func (u *DownsamplingItemHandler) GetExpiredItems() ([]DownsamplingObject, error) {
	var dsListNew []DownsamplingObject
	log.Println("Getting old deployed simulations...")
	dsList, err := u.db.GetDeployedDownsamplePreviewItems()
	if err != nil {
		return dsListNew, err
	}

	for _, d := range dsList {
		end, _ := time.Parse(time.RFC3339, d.PreviewExpiresAt)
		if err != nil {
			return dsListNew, err
		}

		if time.Now().Sub(end).Minutes() > 0 {
			dsListNew = append(dsListNew, d)
		}
	}

	return dsListNew, err
}
