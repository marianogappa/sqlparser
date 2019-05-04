package sqlparser

import (
	"fmt"
	"testing"

	"github.com/marianogappa/sqlparser/query"
	"github.com/stretchr/testify/require"
)

func TestSQL(t *testing.T) {
	ts := []struct {
		name     string
		sql      string
		expected query.Query
		err      error
	}{
		{
			name:     "empty query fails",
			sql:      "",
			expected: query.Query{},
			err:      fmt.Errorf("query type cannot be empty"),
		},
		{
			name:     "SELECT without FROM fails",
			sql:      "SELECT",
			expected: query.Query{Type: query.Select},
			err:      fmt.Errorf("table name cannot be empty"),
		},
		{
			name:     "SELECT without fields fails",
			sql:      "SELECT FROM 'a'",
			expected: query.Query{Type: query.Select},
			err:      fmt.Errorf("at SELECT: expected field to SELECT"),
		},
		{
			name:     "SELECT with comma and empty field fails",
			sql:      "SELECT b, FROM 'a'",
			expected: query.Query{Type: query.Select},
			err:      fmt.Errorf("at SELECT: expected field to SELECT"),
		},
		{
			name:     "SELECT works",
			sql:      "SELECT a FROM 'b'",
			expected: query.Query{Type: query.Select, TableName: "b", Fields: []string{"a"}},
			err:      nil,
		},
		{
			name:     "SELECT works with lowercase",
			sql:      "select a fRoM 'b'",
			expected: query.Query{Type: query.Select, TableName: "b", Fields: []string{"a"}},
			err:      nil,
		},
		{
			name:     "SELECT many fields works",
			sql:      "SELECT a, c, d FROM 'b'",
			expected: query.Query{Type: query.Select, TableName: "b", Fields: []string{"a", "c", "d"}},
			err:      nil,
		},
		{
			name:     "SELECT with empty WHERE fails",
			sql:      "SELECT a, c, d FROM 'b' WHERE",
			expected: query.Query{Type: query.Select, TableName: "b", Fields: []string{"a", "c", "d"}},
			err:      fmt.Errorf("at WHERE: empty WHERE clause"),
		},
		{
			name:     "SELECT with WHERE with only operand fails",
			sql:      "SELECT a, c, d FROM 'b' WHERE a",
			expected: query.Query{Type: query.Select, TableName: "b", Fields: []string{"a", "c", "d"}},
			err:      fmt.Errorf("at WHERE: condition without operator"),
		},
		{
			name: "SELECT with WHERE with = works",
			sql:  "SELECT a, c, d FROM 'b' WHERE a = ''",
			expected: query.Query{
				Type:      query.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []query.Condition{
					{Operand1: "a", Operand1IsField: true, Operator: query.Eq, Operand2: "", Operand2IsField: false},
				},
			},
			err: nil,
		},
		{
			name: "SELECT with WHERE with < works",
			sql:  "SELECT a, c, d FROM 'b' WHERE a < '1'",
			expected: query.Query{
				Type:      query.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []query.Condition{
					{Operand1: "a", Operand1IsField: true, Operator: query.Lt, Operand2: "1", Operand2IsField: false},
				},
			},
			err: nil,
		},
		{
			name: "SELECT with WHERE with <= works",
			sql:  "SELECT a, c, d FROM 'b' WHERE a <= '1'",
			expected: query.Query{
				Type:      query.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []query.Condition{
					{Operand1: "a", Operand1IsField: true, Operator: query.Lte, Operand2: "1", Operand2IsField: false},
				},
			},
			err: nil,
		},
		{
			name: "SELECT with WHERE with > works",
			sql:  "SELECT a, c, d FROM 'b' WHERE a > '1'",
			expected: query.Query{
				Type:      query.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []query.Condition{
					{Operand1: "a", Operand1IsField: true, Operator: query.Gt, Operand2: "1", Operand2IsField: false},
				},
			},
			err: nil,
		},
		{
			name: "SELECT with WHERE with >= works",
			sql:  "SELECT a, c, d FROM 'b' WHERE a >= '1'",
			expected: query.Query{
				Type:      query.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []query.Condition{
					{Operand1: "a", Operand1IsField: true, Operator: query.Gte, Operand2: "1", Operand2IsField: false},
				},
			},
			err: nil,
		},
		{
			name: "SELECT with WHERE with != works",
			sql:  "SELECT a, c, d FROM 'b' WHERE a != '1'",
			expected: query.Query{
				Type:      query.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []query.Condition{
					{Operand1: "a", Operand1IsField: true, Operator: query.Ne, Operand2: "1", Operand2IsField: false},
				},
			},
			err: nil,
		},
		{
			name: "SELECT with WHERE with two conditions using AND works",
			sql:  "SELECT a, c, d FROM 'b' WHERE a != '1' AND b = '2'",
			expected: query.Query{
				Type:      query.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []query.Condition{
					{Operand1: "a", Operand1IsField: true, Operator: query.Ne, Operand2: "1", Operand2IsField: false},
					{Operand1: "b", Operand1IsField: true, Operator: query.Eq, Operand2: "2", Operand2IsField: false},
				},
			},
			err: nil,
		},
		{
			name:     "Empty UPDATE fails",
			sql:      "UPDATE",
			expected: query.Query{},
			err:      fmt.Errorf("table name cannot be empty"),
		},
		{
			name:     "Incomplete UPDATE with table name fails",
			sql:      "UPDATE 'a'",
			expected: query.Query{},
			err:      fmt.Errorf("at WHERE: WHERE clause is mandatory for UPDATE & DELETE"),
		},
		{
			name:     "Incomplete UPDATE with table name and SET fails",
			sql:      "UPDATE 'a' SET",
			expected: query.Query{},
			err:      fmt.Errorf("at WHERE: WHERE clause is mandatory for UPDATE & DELETE"),
		},
		{
			name:     "Incomplete UPDATE with table name, SET with a field but no value and WHERE fails",
			sql:      "UPDATE 'a' SET b WHERE",
			expected: query.Query{},
			err:      fmt.Errorf("at UPDATE: expected '='"),
		},
		{
			name:     "Incomplete UPDATE with table name, SET with a field and = but no value and WHERE fails",
			sql:      "UPDATE 'a' SET b = WHERE",
			expected: query.Query{},
			err:      fmt.Errorf("at UPDATE: expected quoted value"),
		},
		{
			name:     "Incomplete UPDATE due to no WHERE clause fails",
			sql:      "UPDATE 'a' SET b = 'hello' WHERE",
			expected: query.Query{},
			err:      fmt.Errorf("at WHERE: empty WHERE clause"),
		},
		{
			name:     "Incomplete UPDATE due incomplete WHERE clause fails",
			sql:      "UPDATE 'a' SET b = 'hello' WHERE a",
			expected: query.Query{},
			err:      fmt.Errorf("at WHERE: condition without operator"),
		},
		{
			name: "UPDATE works",
			sql:  "UPDATE 'a' SET b = 'hello' WHERE a = '1'",
			expected: query.Query{
				Type:      query.Update,
				TableName: "a",
				Updates:   map[string]string{"b": "hello"},
				Conditions: []query.Condition{
					{Operand1: "a", Operand1IsField: true, Operator: query.Eq, Operand2: "1", Operand2IsField: false},
				},
			},
			err: nil,
		},
		{
			name: "UPDATE with multiple SETs works",
			sql:  "UPDATE 'a' SET b = 'hello', c = 'bye' WHERE a = '1'",
			expected: query.Query{
				Type:      query.Update,
				TableName: "a",
				Updates:   map[string]string{"b": "hello", "c": "bye"},
				Conditions: []query.Condition{
					{Operand1: "a", Operand1IsField: true, Operator: query.Eq, Operand2: "1", Operand2IsField: false},
				},
			},
			err: nil,
		},
		{
			name: "UPDATE with multiple SETs and multiple conditions works",
			sql:  "UPDATE 'a' SET b = 'hello', c = 'bye' WHERE a = '1' AND b = '789'",
			expected: query.Query{
				Type:      query.Update,
				TableName: "a",
				Updates:   map[string]string{"b": "hello", "c": "bye"},
				Conditions: []query.Condition{
					{Operand1: "a", Operand1IsField: true, Operator: query.Eq, Operand2: "1", Operand2IsField: false},
					{Operand1: "b", Operand1IsField: true, Operator: query.Eq, Operand2: "789", Operand2IsField: false},
				},
			},
			err: nil,
		},
		{
			name:     "Empty DELETE fails",
			sql:      "DELETE FROM",
			expected: query.Query{},
			err:      fmt.Errorf("table name cannot be empty"),
		},
		{
			name:     "DELETE without WHERE fails",
			sql:      "DELETE FROM 'a'",
			expected: query.Query{},
			err:      fmt.Errorf("at WHERE: WHERE clause is mandatory for UPDATE & DELETE"),
		},
		{
			name:     "DELETE with empty WHERE fails",
			sql:      "DELETE FROM 'a' WHERE",
			expected: query.Query{},
			err:      fmt.Errorf("at WHERE: empty WHERE clause"),
		},
		{
			name:     "DELETE with WHERE with field but no operator fails",
			sql:      "DELETE FROM 'a' WHERE b",
			expected: query.Query{},
			err:      fmt.Errorf("at WHERE: condition without operator"),
		},
		{
			name: "DELETE with WHERE works",
			sql:  "DELETE FROM 'a' WHERE b = '1'",
			expected: query.Query{
				Type:      query.Delete,
				TableName: "a",
				Conditions: []query.Condition{
					{Operand1: "b", Operand1IsField: true, Operator: query.Eq, Operand2: "1", Operand2IsField: false},
				},
			},
			err: nil,
		},
		{
			name:     "Empty INSERT fails",
			sql:      "INSERT INTO",
			expected: query.Query{},
			err:      fmt.Errorf("table name cannot be empty"),
		},
		{
			name:     "INSERT with no rows to insert fails",
			sql:      "INSERT INTO 'a'",
			expected: query.Query{},
			err:      fmt.Errorf("at INSERT INTO: need at least one row to insert"),
		},
		{
			name:     "INSERT with incomplete value section fails",
			sql:      "INSERT INTO 'a' (",
			expected: query.Query{},
			err:      fmt.Errorf("at INSERT INTO: need at least one row to insert"),
		},
		{
			name:     "INSERT with incomplete value section fails #2",
			sql:      "INSERT INTO 'a' (b",
			expected: query.Query{},
			err:      fmt.Errorf("at INSERT INTO: need at least one row to insert"),
		},
		{
			name:     "INSERT with incomplete value section fails #3",
			sql:      "INSERT INTO 'a' (b)",
			expected: query.Query{},
			err:      fmt.Errorf("at INSERT INTO: need at least one row to insert"),
		},
		{
			name:     "INSERT with incomplete value section fails #4",
			sql:      "INSERT INTO 'a' (b) VALUES",
			expected: query.Query{},
			err:      fmt.Errorf("at INSERT INTO: need at least one row to insert"),
		},
		{
			name:     "INSERT with incomplete row fails",
			sql:      "INSERT INTO 'a' (b) VALUES (",
			expected: query.Query{},
			err:      fmt.Errorf("at INSERT INTO: value count doesn't match field count"),
		},
		{
			name: "INSERT works",
			sql:  "INSERT INTO 'a' (b) VALUES ('1')",
			expected: query.Query{
				Type:      query.Insert,
				TableName: "a",
				Fields:    []string{"b"},
				Inserts:   [][]string{{"1"}},
			},
			err: nil,
		},
		{
			name: "INSERT with multiple fields works",
			sql:  "INSERT INTO 'a' (b,c,    d) VALUES ('1','2' ,  '3' )",
			expected: query.Query{
				Type:      query.Insert,
				TableName: "a",
				Fields:    []string{"b", "c", "d"},
				Inserts:   [][]string{{"1", "2", "3"}},
			},
			err: nil,
		},
		{
			name: "INSERT with multiple fields and multiple values works",
			sql:  "INSERT INTO 'a' (b,c,    d) VALUES ('1','2' ,  '3' ),('4','5' ,'6' )",
			expected: query.Query{
				Type:      query.Insert,
				TableName: "a",
				Fields:    []string{"b", "c", "d"},
				Inserts:   [][]string{{"1", "2", "3"}, {"4", "5", "6"}},
			},
			err: nil,
		},
	}
	for _, tc := range ts {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := ParseMany([]string{tc.sql})
			if tc.err != nil && err == nil {
				t.Errorf("Error should have been %v", tc.err)
			}
			if tc.err == nil && err != nil {
				t.Errorf("Error should have been nil but was %v", err)
			}
			if tc.err != nil && err != nil {
				require.Equal(t, tc.err, err, "Unexpected error")
			}
			if len(actual) > 0 {
				require.Equal(t, tc.expected, actual[0], "Query didn't match expectation")
			}
		})
	}
}
