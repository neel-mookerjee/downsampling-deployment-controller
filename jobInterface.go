package main

type DownsampleJobInterface interface {
	Execute(params PARAM) error
}
