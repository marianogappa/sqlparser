package sqlparser

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"text/template"

	"github.com/marianogappa/sqlparser/query"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	Name     string
	SQL      string
	Expected query.Query
	Err      error
}

type output struct {
	NoErrorExamples []testCase
	ErrorExamples   []testCase
	Types           []string
	Operators       []string
}

func TestSQL(t *testing.T) {
	ts := []testCase{
		{
			Name:     "empty query fails",
			SQL:      "",
			Expected: query.Query{},
			Err:      fmt.Errorf("query type cannot be empty"),
		},
		{
			Name:     "SELECT without FROM fails",
			SQL:      "SELECT",
			Expected: query.Query{Type: query.Select},
			Err:      fmt.Errorf("table name cannot be empty"),
		},
		{
			Name:     "SELECT without fields fails",
			SQL:      "SELECT FROM 'a'",
			Expected: query.Query{Type: query.Select},
			Err:      fmt.Errorf("at SELECT: expected field to SELECT"),
		},
		{
			Name:     "SELECT with comma and empty field fails",
			SQL:      "SELECT b, FROM 'a'",
			Expected: query.Query{Type: query.Select},
			Err:      fmt.Errorf("at SELECT: expected field to SELECT"),
		},
		{
			Name:     "SELECT works",
			SQL:      "SELECT a FROM 'b'",
			Expected: query.Query{Type: query.Select, TableName: "b", Fields: []string{"a"}},
			Err:      nil,
		},
		{
			Name:     "SELECT works with lowercase",
			SQL:      "select a fRoM 'b'",
			Expected: query.Query{Type: query.Select, TableName: "b", Fields: []string{"a"}},
			Err:      nil,
		},
		{
			Name:     "SELECT many fields works",
			SQL:      "SELECT a, c, d FROM 'b'",
			Expected: query.Query{Type: query.Select, TableName: "b", Fields: []string{"a", "c", "d"}},
			Err:      nil,
		},
		{
			Name:     "SELECT with empty WHERE fails",
			SQL:      "SELECT a, c, d FROM 'b' WHERE",
			Expected: query.Query{Type: query.Select, TableName: "b", Fields: []string{"a", "c", "d"}},
			Err:      fmt.Errorf("at WHERE: empty WHERE clause"),
		},
		{
			Name:     "SELECT with WHERE with only operand fails",
			SQL:      "SELECT a, c, d FROM 'b' WHERE a",
			Expected: query.Query{Type: query.Select, TableName: "b", Fields: []string{"a", "c", "d"}},
			Err:      fmt.Errorf("at WHERE: condition without operator"),
		},
		{
			Name: "SELECT with WHERE with = works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a = ''",
			Expected: query.Query{
				Type:      query.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []query.Condition{
					{Operand1: "a", Operand1IsField: true, Operator: query.Eq, Operand2: "", Operand2IsField: false},
				},
			},
			Err: nil,
		},
		{
			Name: "SELECT with WHERE with < works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a < '1'",
			Expected: query.Query{
				Type:      query.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []query.Condition{
					{Operand1: "a", Operand1IsField: true, Operator: query.Lt, Operand2: "1", Operand2IsField: false},
				},
			},
			Err: nil,
		},
		{
			Name: "SELECT with WHERE with <= works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a <= '1'",
			Expected: query.Query{
				Type:      query.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []query.Condition{
					{Operand1: "a", Operand1IsField: true, Operator: query.Lte, Operand2: "1", Operand2IsField: false},
				},
			},
			Err: nil,
		},
		{
			Name: "SELECT with WHERE with > works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a > '1'",
			Expected: query.Query{
				Type:      query.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []query.Condition{
					{Operand1: "a", Operand1IsField: true, Operator: query.Gt, Operand2: "1", Operand2IsField: false},
				},
			},
			Err: nil,
		},
		{
			Name: "SELECT with WHERE with >= works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a >= '1'",
			Expected: query.Query{
				Type:      query.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []query.Condition{
					{Operand1: "a", Operand1IsField: true, Operator: query.Gte, Operand2: "1", Operand2IsField: false},
				},
			},
			Err: nil,
		},
		{
			Name: "SELECT with WHERE with != works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a != '1'",
			Expected: query.Query{
				Type:      query.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []query.Condition{
					{Operand1: "a", Operand1IsField: true, Operator: query.Ne, Operand2: "1", Operand2IsField: false},
				},
			},
			Err: nil,
		},
		{
			Name: "SELECT * works",
			SQL:  "SELECT * FROM 'b'",
			Expected: query.Query{
				Type:       query.Select,
				TableName:  "b",
				Fields:     []string{"*"},
				Conditions: nil,
			},
			Err: nil,
		},
		{
			Name: "SELECT a, * works",
			SQL:  "SELECT a, * FROM 'b'",
			Expected: query.Query{
				Type:       query.Select,
				TableName:  "b",
				Fields:     []string{"a", "*"},
				Conditions: nil,
			},
			Err: nil,
		},
		{
			Name: "SELECT with WHERE with two conditions using AND works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a != '1' AND b = '2'",
			Expected: query.Query{
				Type:      query.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []query.Condition{
					{Operand1: "a", Operand1IsField: true, Operator: query.Ne, Operand2: "1", Operand2IsField: false},
					{Operand1: "b", Operand1IsField: true, Operator: query.Eq, Operand2: "2", Operand2IsField: false},
				},
			},
			Err: nil,
		},
		{
			Name:     "Empty UPDATE fails",
			SQL:      "UPDATE",
			Expected: query.Query{},
			Err:      fmt.Errorf("table name cannot be empty"),
		},
		{
			Name:     "Incomplete UPDATE with table name fails",
			SQL:      "UPDATE 'a'",
			Expected: query.Query{},
			Err:      fmt.Errorf("at WHERE: WHERE clause is mandatory for UPDATE & DELETE"),
		},
		{
			Name:     "Incomplete UPDATE with table name and SET fails",
			SQL:      "UPDATE 'a' SET",
			Expected: query.Query{},
			Err:      fmt.Errorf("at WHERE: WHERE clause is mandatory for UPDATE & DELETE"),
		},
		{
			Name:     "Incomplete UPDATE with table name, SET with a field but no value and WHERE fails",
			SQL:      "UPDATE 'a' SET b WHERE",
			Expected: query.Query{},
			Err:      fmt.Errorf("at UPDATE: expected '='"),
		},
		{
			Name:     "Incomplete UPDATE with table name, SET with a field and = but no value and WHERE fails",
			SQL:      "UPDATE 'a' SET b = WHERE",
			Expected: query.Query{},
			Err:      fmt.Errorf("at UPDATE: expected quoted value"),
		},
		{
			Name:     "Incomplete UPDATE due to no WHERE clause fails",
			SQL:      "UPDATE 'a' SET b = 'hello' WHERE",
			Expected: query.Query{},
			Err:      fmt.Errorf("at WHERE: empty WHERE clause"),
		},
		{
			Name:     "Incomplete UPDATE due incomplete WHERE clause fails",
			SQL:      "UPDATE 'a' SET b = 'hello' WHERE a",
			Expected: query.Query{},
			Err:      fmt.Errorf("at WHERE: condition without operator"),
		},
		{
			Name: "UPDATE works",
			SQL:  "UPDATE 'a' SET b = 'hello' WHERE a = '1'",
			Expected: query.Query{
				Type:      query.Update,
				TableName: "a",
				Updates:   map[string]string{"b": "hello"},
				Conditions: []query.Condition{
					{Operand1: "a", Operand1IsField: true, Operator: query.Eq, Operand2: "1", Operand2IsField: false},
				},
			},
			Err: nil,
		},
		{
			Name: "UPDATE with multiple SETs works",
			SQL:  "UPDATE 'a' SET b = 'hello', c = 'bye' WHERE a = '1'",
			Expected: query.Query{
				Type:      query.Update,
				TableName: "a",
				Updates:   map[string]string{"b": "hello", "c": "bye"},
				Conditions: []query.Condition{
					{Operand1: "a", Operand1IsField: true, Operator: query.Eq, Operand2: "1", Operand2IsField: false},
				},
			},
			Err: nil,
		},
		{
			Name: "UPDATE with multiple SETs and multiple conditions works",
			SQL:  "UPDATE 'a' SET b = 'hello', c = 'bye' WHERE a = '1' AND b = '789'",
			Expected: query.Query{
				Type:      query.Update,
				TableName: "a",
				Updates:   map[string]string{"b": "hello", "c": "bye"},
				Conditions: []query.Condition{
					{Operand1: "a", Operand1IsField: true, Operator: query.Eq, Operand2: "1", Operand2IsField: false},
					{Operand1: "b", Operand1IsField: true, Operator: query.Eq, Operand2: "789", Operand2IsField: false},
				},
			},
			Err: nil,
		},
		{
			Name:     "Empty DELETE fails",
			SQL:      "DELETE FROM",
			Expected: query.Query{},
			Err:      fmt.Errorf("table name cannot be empty"),
		},
		{
			Name:     "DELETE without WHERE fails",
			SQL:      "DELETE FROM 'a'",
			Expected: query.Query{},
			Err:      fmt.Errorf("at WHERE: WHERE clause is mandatory for UPDATE & DELETE"),
		},
		{
			Name:     "DELETE with empty WHERE fails",
			SQL:      "DELETE FROM 'a' WHERE",
			Expected: query.Query{},
			Err:      fmt.Errorf("at WHERE: empty WHERE clause"),
		},
		{
			Name:     "DELETE with WHERE with field but no operator fails",
			SQL:      "DELETE FROM 'a' WHERE b",
			Expected: query.Query{},
			Err:      fmt.Errorf("at WHERE: condition without operator"),
		},
		{
			Name: "DELETE with WHERE works",
			SQL:  "DELETE FROM 'a' WHERE b = '1'",
			Expected: query.Query{
				Type:      query.Delete,
				TableName: "a",
				Conditions: []query.Condition{
					{Operand1: "b", Operand1IsField: true, Operator: query.Eq, Operand2: "1", Operand2IsField: false},
				},
			},
			Err: nil,
		},
		{
			Name:     "Empty INSERT fails",
			SQL:      "INSERT INTO",
			Expected: query.Query{},
			Err:      fmt.Errorf("table name cannot be empty"),
		},
		{
			Name:     "INSERT with no rows to insert fails",
			SQL:      "INSERT INTO 'a'",
			Expected: query.Query{},
			Err:      fmt.Errorf("at INSERT INTO: need at least one row to insert"),
		},
		{
			Name:     "INSERT with incomplete value section fails",
			SQL:      "INSERT INTO 'a' (",
			Expected: query.Query{},
			Err:      fmt.Errorf("at INSERT INTO: need at least one row to insert"),
		},
		{
			Name:     "INSERT with incomplete value section fails #2",
			SQL:      "INSERT INTO 'a' (b",
			Expected: query.Query{},
			Err:      fmt.Errorf("at INSERT INTO: need at least one row to insert"),
		},
		{
			Name:     "INSERT with incomplete value section fails #3",
			SQL:      "INSERT INTO 'a' (b)",
			Expected: query.Query{},
			Err:      fmt.Errorf("at INSERT INTO: need at least one row to insert"),
		},
		{
			Name:     "INSERT with incomplete value section fails #4",
			SQL:      "INSERT INTO 'a' (b) VALUES",
			Expected: query.Query{},
			Err:      fmt.Errorf("at INSERT INTO: need at least one row to insert"),
		},
		{
			Name:     "INSERT with incomplete row fails",
			SQL:      "INSERT INTO 'a' (b) VALUES (",
			Expected: query.Query{},
			Err:      fmt.Errorf("at INSERT INTO: value count doesn't match field count"),
		},
		{
			Name: "INSERT works",
			SQL:  "INSERT INTO 'a' (b) VALUES ('1')",
			Expected: query.Query{
				Type:      query.Insert,
				TableName: "a",
				Fields:    []string{"b"},
				Inserts:   [][]string{{"1"}},
			},
			Err: nil,
		},
		{
			Name:     "INSERT * fails",
			SQL:      "INSERT INTO 'a' (*) VALUES ('1')",
			Expected: query.Query{},
			Err:      fmt.Errorf("at INSERT INTO: expected at least one field to insert"),
		},
		{
			Name: "INSERT with multiple fields works",
			SQL:  "INSERT INTO 'a' (b,c,    d) VALUES ('1','2' ,  '3' )",
			Expected: query.Query{
				Type:      query.Insert,
				TableName: "a",
				Fields:    []string{"b", "c", "d"},
				Inserts:   [][]string{{"1", "2", "3"}},
			},
			Err: nil,
		},
		{
			Name: "INSERT with multiple fields and multiple values works",
			SQL:  "INSERT INTO 'a' (b,c,    d) VALUES ('1','2' ,  '3' ),('4','5' ,'6' )",
			Expected: query.Query{
				Type:      query.Insert,
				TableName: "a",
				Fields:    []string{"b", "c", "d"},
				Inserts:   [][]string{{"1", "2", "3"}, {"4", "5", "6"}},
			},
			Err: nil,
		},
	}

	output := output{Types: query.TypeString, Operators: query.OperatorString}
	for _, tc := range ts {
		t.Run(tc.Name, func(t *testing.T) {
			actual, err := ParseMany([]string{tc.SQL})
			if tc.Err != nil && err == nil {
				t.Errorf("Error should have been %v", tc.Err)
			}
			if tc.Err == nil && err != nil {
				t.Errorf("Error should have been nil but was %v", err)
			}
			if tc.Err != nil && err != nil {
				require.Equal(t, tc.Err, err, "Unexpected error")
			}
			if len(actual) > 0 {
				require.Equal(t, tc.Expected, actual[0], "Query didn't match expectation")
			}
			if tc.Err != nil {
				output.ErrorExamples = append(output.ErrorExamples, tc)
			} else {
				output.NoErrorExamples = append(output.NoErrorExamples, tc)
			}
		})
	}
	createReadme(output)
}

func createReadme(out output) {
	content, err := ioutil.ReadFile("README.template")
	if err != nil {
		log.Fatal(err)
	}
	t := template.Must(template.New("").Parse(string(content)))
	f, err := os.Create("README.md")
	if err != nil {
		log.Fatal(err)
	}
	if err := t.Execute(f, out); err != nil {
		log.Fatal(err)
	}
}
