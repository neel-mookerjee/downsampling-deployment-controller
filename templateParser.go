package main

import (
	"bytes"
	"text/template"
)

type DefaultFiller struct {
	StackName string
	Command   string
	Argument  string
	Config    Config
}

type TemplateParserInterface interface {
	LoadTemplate(path string, filler interface{}) (string, error)
}

type TemplateParser struct {
	templateThis *template.Template
}

func NewTemplateParser() *TemplateParser {
	return &TemplateParser{template.New("parser")}
}

func (t *TemplateParser) LoadTemplate(path string, filler interface{}) (string, error) {
	var output string
	tmpl, err := template.ParseFiles(path)
	if err != nil {
		return output, err
	}

	buf := new(bytes.Buffer)
	err = tmpl.Execute(buf, filler)
	if err != nil {
		return output, err
	}

	return buf.String(), nil
}
