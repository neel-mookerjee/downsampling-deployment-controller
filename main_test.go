package main

import (
	"fmt"
	"strings"
	"testing"
)

type MainTestSuite struct {
	mode            string
	operation       string
	label           string
	expectedMessage string
}

var MainTestCases = []MainTestSuite{
	{
		"local",
		"coordinate",
		"mode:local",
		"",
	},
	{
		"in-cluster",
		"coordinate",
		"mode:in-cluster",
		"",
	},
	{
		"crap",
		"coordinate",
		"mode:wrong",
		"invalid mode",
	},
	{
		"local",
		"coordinate",
		"operation:coordinate",
		"",
	},
	{
		"local",
		"simulate",
		"operation:simulate",
		"",
	},
	{
		"local",
		"deploy",
		"operation:deploy",
		"",
	},
	{
		"local",
		"delete",
		"operation:delete",
		"",
	},
	{
		"local",
		"expire",
		"operation:expire",
		"",
	},
	{
		"local",
		"crap",
		"operation:crap",
		"invalid operation",
	},
}

func Test_Main_ValidateParams(t *testing.T) {
	for _, testCase := range MainTestCases {
		t.Run(testCase.label, func(t *testing.T) {
			PARAMS = &PARAM{MODE_IN_CLUSTER: "in-cluster", MODE_LOCAL: "local", OPERATION_COORDINATE: "coordinate", OPERATION_SIMULATE: "simulate", OPERATION_DEPLOY: "deploy", OPERATION_DELETE: "delete", OPERATION_EXPIRE: "expire", mode: testCase.mode, operation: testCase.operation}
			err := ValidateParams()
			testCase.AssertErrorNotExpected(err, t)
			testCase.AssertError(err, t)
		})
	}
}

func (suite *MainTestSuite) AssertErrorNotExpected(err error, t *testing.T) {
	if suite.expectedMessage == "" && err != nil {
		t.Error(fmt.Sprintf("%s: Recieved error that should have not been received. %v", suite.label, err))
	}
}

func (suite *MainTestSuite) AssertError(err error, t *testing.T) {
	if suite.expectedMessage != "" && (err == nil || !strings.Contains(err.Error(), suite.expectedMessage)) {
		t.Error(fmt.Sprintf("%s: Recieved error that should have not been received. %v", suite.label, err))
	}
}
