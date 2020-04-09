package main

import "fmt"
import (
	"strings"
	"testing"
)

type TemplateParserTestSuite struct {
	TemplateParser TemplateParser
	Filler         DashboardTemplateFiller
}

func NewTemplateParserTestSuite() *TemplateParserTestSuite {
	filler := &DashboardTemplateFiller{"db-test123", "tm-test123", "tag-test123", "field-test123"}

	return &TemplateParserTestSuite{TemplateParser{}, *filler}
}

func Test_TemplateParser_LoadTemplate(t *testing.T) {
	tc := NewTemplateParserTestSuite()
	str, err := tc.TemplateParser.LoadTemplate("templates/dashboard.json", tc.Filler)
	if err != nil {
		t.Error(fmt.Sprintf("Error wasn't expected but received. %v", err))
	}
	if !strings.Contains(str, "db-test123") || !strings.Contains(str, "tm-test123") || !strings.Contains(str, "tag-test123") || !strings.Contains(str, "field-test123") {
		t.Error(fmt.Sprintf("Expected filling values are not present in the parser output"))
	}
}
