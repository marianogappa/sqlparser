package sqlparser

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/marianogappa/sqlparser/query"
)

// Parse takes a string representing a SQL query and parses it into a query.Query struct. It may fail.
func Parse(sqls string) (query.Query, error) {
	qs, err := ParseMany([]string{sqls})
	if len(qs) == 0 {
		return query.Query{}, err
	}
	return qs[0], err
}

// ParseMany takes a string slice representing many SQL queries and parses them into a query.Query struct slice.
// It may fail. If it fails, it will stop at the first failure.
func ParseMany(sqls []string) ([]query.Query, error) {
	qs := []query.Query{}
	for _, sql := range sqls {
		q, err := parse(sql)
		if err != nil {
			return qs, err
		}
		qs = append(qs, q)
	}
	return qs, nil
}

func parse(sql string) (query.Query, error) {
	sql = strings.TrimSpace(sql)
	return (&parser{0, 0, "", sql, strings.ToUpper(sql), stepType, query.Query{}, nil, ""}).parse()
}

type step int

const (
	stepType step = iota
	stepSelectField
	stepSelectFrom
	stepSelectComma
	stepSelectFromTable
	stepInsertTable
	stepInsertFieldsOpeningParens
	stepInsertFields
	stepInsertFieldsCommaOrClosingParens
	stepInsertValuesOpeningParens
	stepInsertValuesRWord
	stepInsertValues
	stepInsertValuesCommaOrClosingParens
	stepInsertValuesCommaBeforeOpeningParens
	stepUpdateTable
	stepUpdateSet
	stepUpdateField
	stepUpdateEquals
	stepUpdateValue
	stepUpdateComma
	stepDeleteFromTable
	stepWhere
	stepWhereField
	stepWhereOperator
	stepWhereValue
	stepWhereAnd
)

type parser struct {
	i               int
	len             int
	peeked          string
	sql             string
	sqlUpper        string
	step            step
	query           query.Query
	err             error
	nextUpdateField string
}

func (p *parser) parse() (query.Query, error) {
	q, err := p.doParse()
	p.err = err
	if p.err == nil {
		p.err = p.validate()
	}
	p.logError()
	return q, p.err
}

func (p *parser) doParse() (query.Query, error) {
	for {
		if p.i >= len(p.sql) {
			return p.query, p.err
		}
		switch p.step {
		case stepType:
			s := p.peek(true)
			switch s {
			case "SELECT":
				p.query.Type = query.Select
				p.step = stepSelectField
			case "INSERT INTO":
				p.query.Type = query.Insert
				p.step = stepInsertTable
			case "UPDATE":
				p.query.Type = query.Update
				p.query.Updates = map[string]string{}
				p.step = stepUpdateTable
			case "DELETE FROM":
				p.query.Type = query.Delete
				p.step = stepDeleteFromTable
			default:
				return p.query, fmt.Errorf("invalid query type")
			}
			p.pop()
		case stepSelectField:
			identifier := p.peek(false)
			if !isIdentifierOrAsterisk(identifier) {
				return p.query, fmt.Errorf("at SELECT: expected field to SELECT")
			}
			p.query.Fields = append(p.query.Fields, identifier)
			p.pop()
			maybeFrom := p.peek(true)
			if maybeFrom == "AS" {
				// alias
				p.pop()
				alias := p.peek(false)
				if !isIdentifierOrAsterisk(alias) {
					return p.query, fmt.Errorf("at SELECT: expected alias (AS) for %s", identifier)
				}
				p.query.Aliases = append(p.query.Aliases, alias)
				p.pop()
				maybeFrom = p.peek(true)
			} else {
				p.query.Aliases = append(p.query.Aliases, "")
			}
			if maybeFrom == "FROM" {
				p.step = stepSelectFrom
				continue
			}
			p.step = stepSelectComma
		case stepSelectComma:
			commaRWord := p.peek(false)
			if commaRWord != "," {
				return p.query, fmt.Errorf("at SELECT: expected comma or FROM")
			}
			p.pop()
			p.step = stepSelectField
		case stepSelectFrom:
			fromRWord := p.peek(true)
			if fromRWord != "FROM" {
				return p.query, fmt.Errorf("at SELECT: expected FROM")
			}
			p.pop()
			p.step = stepSelectFromTable
		case stepSelectFromTable:
			tableName := p.peek(false)
			if len(tableName) == 0 {
				return p.query, fmt.Errorf("at SELECT: expected quoted table name")
			}
			p.query.TableName = tableName
			p.pop()
			p.step = stepWhere
		case stepInsertTable:
			tableName := p.peek(false)
			if len(tableName) == 0 {
				return p.query, fmt.Errorf("at INSERT INTO: expected quoted table name")
			}
			p.query.TableName = tableName
			p.pop()
			p.step = stepInsertFieldsOpeningParens
		case stepDeleteFromTable:
			tableName := p.peek(false)
			if len(tableName) == 0 {
				return p.query, fmt.Errorf("at DELETE FROM: expected quoted table name")
			}
			p.query.TableName = tableName
			p.pop()
			p.step = stepWhere
		case stepUpdateTable:
			tableName := p.peek(false)
			if len(tableName) == 0 {
				return p.query, fmt.Errorf("at UPDATE: expected quoted table name")
			}
			p.query.TableName = tableName
			p.pop()
			p.step = stepUpdateSet
		case stepUpdateSet:
			setRWord := p.peek(true)
			if setRWord != "SET" {
				return p.query, fmt.Errorf("at UPDATE: expected 'SET'")
			}
			p.pop()
			p.step = stepUpdateField
		case stepUpdateField:
			identifier := p.peek(false)
			if !isIdentifier(identifier) {
				return p.query, fmt.Errorf("at UPDATE: expected at least one field to update")
			}
			p.nextUpdateField = identifier
			p.pop()
			p.step = stepUpdateEquals
		case stepUpdateEquals:
			equalsRWord := p.peek(false)
			if equalsRWord != "=" {
				return p.query, fmt.Errorf("at UPDATE: expected '='")
			}
			p.pop()
			p.step = stepUpdateValue
		case stepUpdateValue:
			quotedValue := p.peekQuotedString(false)
			if p.len == 0 {
				return p.query, fmt.Errorf("at UPDATE: expected quoted value")
			}
			p.query.Updates[p.nextUpdateField] = quotedValue
			p.nextUpdateField = ""
			p.pop()
			maybeWhere := p.peek(true)
			if maybeWhere == "WHERE" {
				p.step = stepWhere
				continue
			}
			p.step = stepUpdateComma
		case stepUpdateComma:
			commaRWord := p.peek(false)
			if commaRWord != "," {
				return p.query, fmt.Errorf("at UPDATE: expected ','")
			}
			p.pop()
			p.step = stepUpdateField
		case stepWhere:
			whereRWord := p.peek(true)
			if whereRWord != "WHERE" {
				return p.query, fmt.Errorf("expected WHERE")
			}
			p.pop()
			p.step = stepWhereField
		case stepWhereField:
			identifier := p.peek(false)
			if !isIdentifier(identifier) {
				return p.query, fmt.Errorf("at WHERE: expected field")
			}
			p.query.Conditions = append(p.query.Conditions, query.Condition{Operand1: identifier, Operand1IsField: true})
			p.pop()
			p.step = stepWhereOperator
		case stepWhereOperator:
			operator := p.peek(false)
			currentCondition := p.query.Conditions[len(p.query.Conditions)-1]
			switch operator {
			case "=":
				currentCondition.Operator = query.Eq
			case ">":
				currentCondition.Operator = query.Gt
			case ">=":
				currentCondition.Operator = query.Gte
			case "<":
				currentCondition.Operator = query.Lt
			case "<=":
				currentCondition.Operator = query.Lte
			case "!=":
				currentCondition.Operator = query.Ne
			default:
				return p.query, fmt.Errorf("at WHERE: unknown operator")
			}
			p.query.Conditions[len(p.query.Conditions)-1] = currentCondition
			p.pop()
			p.step = stepWhereValue
		case stepWhereValue:
			currentCondition := p.query.Conditions[len(p.query.Conditions)-1]
			identifier := p.peek(false)
			if isIdentifier(identifier) {
				currentCondition.Operand2 = identifier
				currentCondition.Operand2IsField = true
			} else {
				quotedValue := p.peekQuotedString(false)
				if p.len == 0 {
					return p.query, fmt.Errorf("at WHERE: expected quoted value")
				}
				currentCondition.Operand2 = quotedValue
				currentCondition.Operand2IsField = false
			}
			p.query.Conditions[len(p.query.Conditions)-1] = currentCondition
			p.pop()
			p.step = stepWhereAnd
		case stepWhereAnd:
			andRWord := p.peek(true)
			if andRWord != "AND" {
				return p.query, fmt.Errorf("expected AND")
			}
			p.pop()
			p.step = stepWhereField
		case stepInsertFieldsOpeningParens:
			openingParens := p.peek(false)
			if len(openingParens) != 1 || openingParens != "(" {
				return p.query, fmt.Errorf("at INSERT INTO: expected opening parens")
			}
			p.pop()
			p.step = stepInsertFields
		case stepInsertFields:
			identifier := p.peek(false)
			if !isIdentifier(identifier) {
				return p.query, fmt.Errorf("at INSERT INTO: expected at least one field to insert")
			}
			p.query.Fields = append(p.query.Fields, identifier)
			p.pop()
			p.step = stepInsertFieldsCommaOrClosingParens
		case stepInsertFieldsCommaOrClosingParens:
			commaOrClosingParens := p.peek(false)
			if commaOrClosingParens != "," && commaOrClosingParens != ")" {
				return p.query, fmt.Errorf("at INSERT INTO: expected comma or closing parens")
			}
			p.pop()
			if commaOrClosingParens == "," {
				p.step = stepInsertFields
				continue
			}
			p.step = stepInsertValuesRWord
		case stepInsertValuesRWord:
			valuesRWord := p.peek(true)
			if valuesRWord != "VALUES" {
				return p.query, fmt.Errorf("at INSERT INTO: expected 'VALUES'")
			}
			p.pop()
			p.step = stepInsertValuesOpeningParens
		case stepInsertValuesOpeningParens:
			openingParens := p.peek(false)
			if openingParens != "(" {
				return p.query, fmt.Errorf("at INSERT INTO: expected opening parens")
			}
			p.query.Inserts = append(p.query.Inserts, []string{})
			p.pop()
			p.step = stepInsertValues
		case stepInsertValues:
			quotedValue := p.peekQuotedString(false)
			if p.len == 0 {
				return p.query, fmt.Errorf("at INSERT INTO: expected quoted value")
			}
			p.query.Inserts[len(p.query.Inserts)-1] = append(p.query.Inserts[len(p.query.Inserts)-1], quotedValue)
			p.pop()
			p.step = stepInsertValuesCommaOrClosingParens
		case stepInsertValuesCommaOrClosingParens:
			commaOrClosingParens := p.peek(false)
			if commaOrClosingParens != "," && commaOrClosingParens != ")" {
				return p.query, fmt.Errorf("at INSERT INTO: expected comma or closing parens")
			}
			p.pop()
			if commaOrClosingParens == "," {
				p.step = stepInsertValues
				continue
			}
			currentInsertRow := p.query.Inserts[len(p.query.Inserts)-1]
			if len(currentInsertRow) < len(p.query.Fields) {
				return p.query, fmt.Errorf("at INSERT INTO: value count doesn't match field count")
			}
			p.step = stepInsertValuesCommaBeforeOpeningParens
		case stepInsertValuesCommaBeforeOpeningParens:
			commaRWord := p.peek(false)
			if commaRWord != "," {
				return p.query, fmt.Errorf("at INSERT INTO: expected comma")
			}
			p.pop()
			p.step = stepInsertValuesOpeningParens
		}
	}
}

func (p *parser) peek(upper bool) string {
	p.peeked, p.len = p.peekWithLength(upper)
	return p.peeked
}

func (p *parser) peekQuotedString(upper bool) string {
	p.peeked, p.len = p.peekQuotedStringWithLength(upper)
	return p.peeked
}

func (p *parser) peekIdentifier(upper bool) string {
	p.peeked, p.len = p.peekQuotedStringWithLength(upper)
	return p.peeked
}

func (p *parser) pop() string {
	peeked := p.peeked
	p.peeked = ""
	p.i += p.len
	p.len = 0
	p.popWhitespace()
	return peeked
}

func (p *parser) popWithLength(len int) {
	p.i += len
	p.popWhitespace()
}

func (p *parser) popWhitespace() {
	for ; p.i < len(p.sql) && p.sql[p.i] == ' '; p.i++ {
	}
}

var reservedWords = []string{
	"(", ")", ">=", "<=", "!=", ",", "=", ">", "<", "AS", "SELECT", "INSERT INTO", "VALUES", "UPDATE", "DELETE FROM",
	"WHERE", "FROM", "SET",
}

func (p *parser) peekWithLength(upper bool) (string, int) {
	if p.i >= len(p.sql) {
		return "", 0
	}
	for _, rWord := range reservedWords {
		token := p.sqlUpper[p.i:min(len(p.sqlUpper), p.i+len(rWord))]
		if token == rWord {
			if !upper {
				token = p.sql[p.i:min(len(p.sql), p.i+len(rWord))]
			}

			return token, len(token)
		}
	}
	if p.sql[p.i] == '\'' { // Quoted string
		return p.peekQuotedStringWithLength(upper)
	}
	return p.peekIdentifierWithLength(upper)
}

func (p *parser) peekQuotedStringWithLength(upper bool) (string, int) {
	if len(p.sql) < p.i || p.sql[p.i] != '\'' {
		return "", 0
	}
	for i := p.i + 1; i < len(p.sql); i++ {
		if p.sql[i] == '\'' && p.sql[i-1] != '\\' {
			if upper {
				return p.sqlUpper[p.i+1 : i], len(p.sqlUpper[p.i+1:i]) + 2 // +2 for the two quotes
			}
			return p.sql[p.i+1 : i], len(p.sql[p.i+1:i]) + 2 // +2 for the two quotes
		}
	}
	return "", 0
}

func (p *parser) peekIdentifierWithLength(upper bool) (string, int) {
	for i := p.i; i < len(p.sql); i++ {
		isIdentifierSymbol := (p.sql[i] >= 'a' && p.sql[i] <= 'z') ||
			(p.sql[i] >= 'A' && p.sql[i] <= 'Z') ||
			(p.sql[i] >= '0' && p.sql[i] <= '9') ||
			p.sql[i] == '*' ||
			p.sql[i] == '_'
		if !isIdentifierSymbol {
			if p.sql[i] == '(' {
				// detect function
				if end := strings.IndexByte(p.sql[i+1:], ')'); end >= 0 {
					i += end + 2
				}
			}
			if upper {
				return p.sqlUpper[p.i:i], len(p.sqlUpper[p.i:i])
			}
			return p.sql[p.i:i], len(p.sql[p.i:i])
		}
	}
	if upper {
		return p.sqlUpper[p.i:], len(p.sqlUpper[p.i:])
	}
	return p.sql[p.i:], len(p.sql[p.i:])
}

func (p *parser) validate() error {
	if len(p.query.Conditions) == 0 && p.step == stepWhereField {
		return fmt.Errorf("at WHERE: empty WHERE clause")
	}
	if p.query.Type == query.UnknownType {
		return fmt.Errorf("query type cannot be empty")
	}
	if (p.query.Type != query.Select || len(p.query.Fields) == 0) && p.query.TableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}
	if len(p.query.Conditions) == 0 && (p.query.Type == query.Update || p.query.Type == query.Delete) {
		return fmt.Errorf("at WHERE: WHERE clause is mandatory for UPDATE & DELETE")
	}
	for _, c := range p.query.Conditions {
		if c.Operator == query.UnknownOperator {
			return fmt.Errorf("at WHERE: condition without operator")
		}
		if c.Operand1 == "" && c.Operand1IsField {
			return fmt.Errorf("at WHERE: condition with empty left side operand")
		}
		if c.Operand2 == "" && c.Operand2IsField {
			return fmt.Errorf("at WHERE: condition with empty right side operand")
		}
	}
	if p.query.Type == query.Insert && len(p.query.Inserts) == 0 {
		return fmt.Errorf("at INSERT INTO: need at least one row to insert")
	}
	if p.query.Type == query.Insert {
		for _, i := range p.query.Inserts {
			if len(i) != len(p.query.Fields) {
				return fmt.Errorf("at INSERT INTO: value count doesn't match field count")
			}
		}
	}
	if p.query.Type == query.Select && len(p.query.Fields) != len(p.query.Aliases) {
		return fmt.Errorf("fileds and aliases count mismatch")
	}
	return nil
}

func (p *parser) logError() {
	if p.err == nil {
		return
	}
	fmt.Println(p.sql)
	fmt.Println(strings.Repeat(" ", p.i) + "^")
	fmt.Println(p.err)
}

var regexIdentifier = regexp.MustCompile("[a-zA-Z_][a-zA-Z_0-9]*")

func isIdentifier(s string) bool {
	u := strings.ToUpper(s)
	for _, rw := range reservedWords {
		if u == rw {
			return false
		}
	}
	return regexIdentifier.MatchString(s)
}

func isIdentifierOrAsterisk(s string) bool {
	return s == "*" || isIdentifier(s)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
