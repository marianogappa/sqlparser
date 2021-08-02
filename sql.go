package sqlparser

import (
	"fmt"
	"io"
	"strings"

	"github.com/msaf1980/sqlparser/query"
)

type ErrorWithPos struct {
	msg string
	pos int
}

func newError(pos int, msg string) *ErrorWithPos {
	return &ErrorWithPos{
		msg: msg,
		pos: pos,
	}
}

func newErrorf(pos int, format string, a ...interface{}) *ErrorWithPos {
	return &ErrorWithPos{
		msg: fmt.Sprintf(format, a...),
		pos: pos,
	}
}

func (e *ErrorWithPos) Error() string {
	return e.msg
}

func (e *ErrorWithPos) Pos() int {
	return e.pos
}

func (e *ErrorWithPos) PrintPosError(sql string, w io.Writer) {
	fmt.Fprintln(w, sql)
	fmt.Fprintln(w, strings.Repeat(" ", e.pos)+"^")
	fmt.Println(e.msg)
}

// Parse takes a string representing a SQL query and parses it into a query.Query struct. It may fail.
func Parse(sql string) (query.Query, error) {
	sql = strings.TrimSpace(sql)
	return (&parser{
		sql:      sql,
		sqlUpper: strings.ToUpper(sql),
		step:     stepType,
	}).parse()
}

// ParseMany takes a string slice representing many SQL queries and parses them into a query.Query struct slice.
// It may fail. If it fails, it will stop at the first failure.
func ParseMany(sqls []string) ([]query.Query, error) {
	qs := []query.Query{}
	for _, sql := range sqls {
		q, err := Parse(sql)
		if err != nil {
			return qs, err
		}
		qs = append(qs, q)
	}
	return qs, nil
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
	peekQuoted      bool
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
			case "INSERT":
				p.pop()
				s = p.peek(true)
				if s != "INTO" {
					return p.query, newErrorf(p.i, "at INSERT: expected INTO, got %s", s)
				}
				p.query.Type = query.Insert
				p.step = stepInsertTable
			case "UPDATE":
				p.query.Type = query.Update
				p.query.Updates = map[string]string{}
				p.step = stepUpdateTable
			case "DELETE":
				p.pop()
				s = p.peek(true)
				if s != "FROM" {
					return p.query, newErrorf(p.i, "at DELETE: expected FROM, got %s", s)
				}
				p.query.Type = query.Delete
				p.step = stepDeleteFromTable
			default:
				return p.query, newError(p.i, "invalid query type")
			}
			p.pop()
		case stepSelectField:
			identifier := p.peek(false)
			if isId, _ := isIdentifierOrAsterisk(identifier); !isId {
				return p.query, newError(p.i, "at SELECT: expected field to SELECT")
			}
			p.query.Fields = append(p.query.Fields, identifier)
			p.pop()
			maybeFrom := p.peek(true)
			if maybeFrom == "AS" {
				// alias
				p.pop()
				alias := p.peek(false)
				if isId, _ := isIdentifierOrAsterisk(alias); !isId {
					return p.query, newErrorf(p.i, "at AS: expected alias for %s", identifier)
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
				return p.query, newError(p.i, "at SELECT: expected comma or FROM")
			}
			p.pop()
			p.step = stepSelectField
		case stepSelectFrom:
			fromRWord := p.peek(true)
			if fromRWord != "FROM" {
				return p.query, newError(p.i, "at SELECT: expected FROM")
			}
			p.pop()
			p.step = stepSelectFromTable
		case stepSelectFromTable:
			tableName := p.peek(false)
			if len(tableName) == 0 {
				return p.query, newError(p.i, "at SELECT: expected quoted table name")
			}
			p.query.TableName = tableName
			p.pop()
			p.step = stepWhere
		case stepInsertTable:
			tableName := p.peek(false)
			if len(tableName) == 0 {
				return p.query, newError(p.i, "at INSERT INTO: expected quoted table name")
			}
			p.query.TableName = tableName
			p.pop()
			p.step = stepInsertFieldsOpeningParens
		case stepDeleteFromTable:
			tableName := p.peek(false)
			if len(tableName) == 0 {
				return p.query, newError(p.i, "at DELETE FROM: expected quoted table name")
			}
			p.query.TableName = tableName
			p.pop()
			p.step = stepWhere
		case stepUpdateTable:
			tableName := p.peek(false)
			if len(tableName) == 0 {
				return p.query, newError(p.i, "at UPDATE: expected quoted table name")
			}
			p.query.TableName = tableName
			p.pop()
			p.step = stepUpdateSet
		case stepUpdateSet:
			setRWord := p.peek(true)
			if setRWord != "SET" {
				return p.query, newError(p.i, "at UPDATE: expected 'SET'")
			}
			p.pop()
			p.step = stepUpdateField
		case stepUpdateField:
			identifier := p.peek(false)
			if isId, _ := isIdentifier(identifier); !isId {
				return p.query, newError(p.i, "at UPDATE: expected at least one field to update")
			}
			p.nextUpdateField = identifier
			p.pop()
			p.step = stepUpdateEquals
		case stepUpdateEquals:
			equalsRWord := p.peek(false)
			if equalsRWord != "=" {
				return p.query, newError(p.i, "at UPDATE: expected '='")
			}
			p.pop()
			p.step = stepUpdateValue
		case stepUpdateValue:
			quotedValue := p.peekQuotedString(false)
			if p.len == 0 {
				return p.query, newError(p.i, "at UPDATE: expected quoted value")
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
				return p.query, newError(p.i, "at UPDATE: expected ','")
			}
			p.pop()
			p.step = stepUpdateField
		case stepWhere:
			whereRWord := p.peek(true)
			if whereRWord != "WHERE" {
				return p.query, newError(p.i, "expected WHERE")
			}
			p.pop()
			p.step = stepWhereField
			if ended, err := p.parseWhere(); ended || err != nil {
				return p.query, err
			}
		case stepInsertFieldsOpeningParens:
			openingParens := p.peek(false)
			if len(openingParens) != 1 || openingParens != "(" {
				return p.query, newError(p.i, "at INSERT INTO: expected opening parens")
			}
			p.pop()
			p.step = stepInsertFields
		case stepInsertFields:
			identifier := p.peek(false)
			if isId, _ := isIdentifier(identifier); !isId {
				return p.query, newError(p.i, "at INSERT INTO: expected at least one field to insert")
			}
			p.query.Fields = append(p.query.Fields, identifier)
			p.pop()
			p.step = stepInsertFieldsCommaOrClosingParens
		case stepInsertFieldsCommaOrClosingParens:
			commaOrClosingParens := p.peek(false)
			if commaOrClosingParens != "," && commaOrClosingParens != ")" {
				return p.query, newError(p.i, "at INSERT INTO: expected comma or closing parens")
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
				return p.query, newError(p.i, "at INSERT INTO: expected 'VALUES'")
			}
			p.pop()
			p.step = stepInsertValuesOpeningParens
		case stepInsertValuesOpeningParens:
			openingParens := p.peek(false)
			if openingParens != "(" {
				return p.query, newError(p.i, "at INSERT INTO: expected opening parens")
			}
			p.query.Inserts = append(p.query.Inserts, []string{})
			p.pop()
			p.step = stepInsertValues
		case stepInsertValues:
			quotedValue := p.peekQuotedString(false)
			if p.len == 0 {
				return p.query, newError(p.i, "at INSERT INTO: expected quoted value")
			}
			p.query.Inserts[len(p.query.Inserts)-1] = append(p.query.Inserts[len(p.query.Inserts)-1], quotedValue)
			p.pop()
			p.step = stepInsertValuesCommaOrClosingParens
		case stepInsertValuesCommaOrClosingParens:
			commaOrClosingParens := p.peek(false)
			if commaOrClosingParens != "," && commaOrClosingParens != ")" {
				return p.query, newError(p.i, "at INSERT INTO: expected comma or closing parens")
			}
			p.pop()
			if commaOrClosingParens == "," {
				p.step = stepInsertValues
				continue
			}
			currentInsertRow := p.query.Inserts[len(p.query.Inserts)-1]
			if len(currentInsertRow) < len(p.query.Fields) {
				return p.query, newError(p.i, "at INSERT INTO: value count doesn't match field count")
			}
			p.step = stepInsertValuesCommaBeforeOpeningParens
		case stepInsertValuesCommaBeforeOpeningParens:
			commaRWord := p.peek(false)
			if commaRWord != "," {
				return p.query, newError(p.i, "at INSERT INTO: expected comma")
			}
			p.pop()
			p.step = stepInsertValuesOpeningParens
		}
	}
}

func (p *parser) parseWhere() (bool, error) {
	for {
		if p.i >= len(p.sql) {
			if len(p.query.Conditions) == 0 {
				return true, newError(p.i, "at WHERE: empty WHERE clause")
			}
			// TODO detect closed

			return true, nil
		}
		switch p.step {
		case stepWhereField:
			identifier := p.peek(false)
			if p.peekQuoted {
				p.query.Conditions = append(p.query.Conditions, query.Condition{Operand1: identifier, Operand1Type: query.OpQuoted})
			} else {
				if len(identifier) == 0 {
					return false, newError(p.i, "at WHERE: empty WHERE clause")
				} else if isId, _ := isIdentifier(identifier); !isId {
					if len(p.query.Conditions) == 0 {
						return true, newError(p.i, "at WHERE: expected field")
					}
					// TODO detect closed

					return true, nil
				}
				p.query.Conditions = append(p.query.Conditions, query.Condition{Operand1: identifier, Operand1Type: query.OpField})
			}
			p.pop()
			p.step = stepWhereOperator
		case stepWhereOperator:
			operatorStr := p.peek(false)
			currentCondition := p.query.Conditions[len(p.query.Conditions)-1]
			operator, _ := reservedWords[operatorStr]
			switch operator {
			case rEQ:
				currentCondition.Operator = query.Eq
			case rGT:
				currentCondition.Operator = query.Gt
			case rGTE:
				currentCondition.Operator = query.Gte
			case rLT:
				currentCondition.Operator = query.Lt
			case rLTE:
				currentCondition.Operator = query.Lte
			case rNE:
				currentCondition.Operator = query.Ne
			default:
				return false, newError(p.i, "at WHERE: unknown operator")
			}
			p.query.Conditions[len(p.query.Conditions)-1] = currentCondition
			p.pop()
			p.step = stepWhereValue
		case stepWhereValue:
			currentCondition := p.query.Conditions[len(p.query.Conditions)-1]
			identifier := p.peek(false)
			if p.peekQuoted {
				currentCondition.Operand2 = identifier
				currentCondition.Operand2Type = query.OpQuoted
			} else {
				if isIdentifier, isNumber := isIdentifier(identifier); isIdentifier {
					currentCondition.Operand2 = identifier
					currentCondition.Operand2Type = query.OpField
				} else if isNumber {
					currentCondition.Operand2 = identifier
					currentCondition.Operand2Type = query.OpNumber
				} else {
					return false, newError(p.i, "at WHERE: expected quoted value")
				}
			}
			p.query.Conditions[len(p.query.Conditions)-1] = currentCondition
			p.pop()
			p.step = stepWhereAnd
		case stepWhereAnd:
			andRWord := p.peek(true)
			if andRWord != "AND" {
				return false, newError(p.i, "expected AND")
			}
			p.pop()
			p.step = stepWhereField
		default:
			// TODO detect closed

			return false, nil
		}
	}
}

func (p *parser) peekCurrent(upper bool) string {
	if upper {
		return p.sqlUpper[p.i : p.i+p.len]
	} else {
		return p.sql[p.i : p.i+p.len]
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
	p.peekQuoted = false
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

type rWord int

const (
	rUnknown rWord = iota
	// reserwed words
	rLeftBracket  // "(""
	rRightBracket // ")"
	rGT           // ">"
	rGTE          // ">="
	rLTE          // "<="
	rLT           // "<"
	rEQ           // "="
	rNE           // "!="
	rCOMMA        // ","
	rSEMI         //";"
	rEX           // "!"
	rAS           // "AS"
	rSELECT       // "SELECT"
	rINSERT       // "INSERT"
	rINTO         //"INTO"
	rVALUES       // "VALUES"
	rUPDATE       // "UPDATE"
	rDELETE       // "DELETE"
	rWHERE        // "WHERE"
	rFROM         // "FROM"
	rSET          // "SET"
	r
)

var (
	reservedSymbols = map[byte]rWord{
		'(': rLeftBracket,
		')': rRightBracket,
		'>': rGT,
		'<': rLT,
		'=': rEQ,
		'!': rEX,
		',': rCOMMA,
		';': rSEMI,
	}

	reservedWords = map[string]rWord{
		"(":      rLeftBracket,
		")":      rRightBracket,
		">":      rGT,
		">=":     rGTE,
		"<":      rLT,
		"<=":     rLTE,
		"=":      rEQ,
		"!=":     rNE,
		",":      rCOMMA,
		";":      rSEMI,
		"AS":     rAS,
		"SELECT": rSELECT,
		"INSERT": rINSERT,
		"INTO":   rINTO,
		"VALUES": rVALUES,
		"UPDATE": rUPDATE,
		"DELETE": rDELETE,
		"FROM":   rFROM,
		"WHERE":  rWHERE,
		"SET":    rSET,
	}
)

func (p *parser) peekWithLength(upper bool) (string, int) {
	if p.i >= len(p.sql) {
		return "", 0
	}

	if p.sql[p.i] == '\'' { // Quoted string
		return p.peekQuotedStringWithLength(upper)
	}

	// for _, rWord := range reservedWords {
	// 	token := p.sqlUpper[p.i:min(len(p.sqlUpper), p.i+len(rWord))]
	// 	if token == rWord {
	// 		if !upper {
	// 			token = p.sql[p.i:min(len(p.sql), p.i+len(rWord))]
	// 		}

	// 		return token, len(token)
	// 	}
	// }

	return p.peekIdentifierWithLength(upper)
}

func (p *parser) peekQuotedStringWithLength(upper bool) (string, int) {
	p.peekQuoted = true
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
	i := p.i
	if _, ok := reservedSymbols[p.sqlUpper[i]]; ok {
		if p.sql[i] == '(' || p.sql[i] == ')' {
			i++
		} else {
			for i = p.i + 1; i < len(p.sql); i++ {
				if _, ok := reservedSymbols[p.sqlUpper[i]]; !ok {
					return p.sql[p.i:i], len(p.sql[p.i:i])
				} else if p.sql[i] == '(' || p.sql[i] == ')' {
					break
				}
			}
		}
		return p.sql[p.i:i], len(p.sql[p.i:i])
	}

	for ; i < len(p.sql); i++ {
		isIdentifierSymbol := (p.sql[i] >= 'a' && p.sql[i] <= 'z') ||
			(p.sql[i] >= 'A' && p.sql[i] <= 'Z') ||
			(p.sql[i] >= '0' && p.sql[i] <= '9') ||
			p.sql[i] == '*' ||
			p.sql[i] == '_' ||
			p.sql[i] == '-' ||
			p.sql[i] == '.'
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
		return newError(p.i, "at WHERE: empty WHERE clause")
	}
	if p.query.Type == query.UnknownType {
		return newError(p.i, "query type cannot be empty")
	}
	if (p.query.Type != query.Select || len(p.query.Fields) == 0) && p.query.TableName == "" {
		return newError(p.i, "table name cannot be empty")
	}
	if len(p.query.Conditions) == 0 && (p.query.Type == query.Update || p.query.Type == query.Delete) {
		return newError(p.i, "at WHERE: WHERE clause is mandatory for UPDATE & DELETE")
	}
	for _, c := range p.query.Conditions {
		if c.Operator == query.UnknownOperator {
			return newError(p.i, "at WHERE: condition without operator")
		}
		if c.Operand1 == "" && c.Operand1Type == query.OpField {
			return newError(p.i, "at WHERE: condition with empty left side operand")
		}
		if c.Operand2 == "" && c.Operand2Type == query.OpField {
			return newError(p.i, "at WHERE: condition with empty right side operand")
		}
	}
	if p.query.Type == query.Insert && len(p.query.Inserts) == 0 {
		return newError(p.i, "at INSERT INTO: need at least one row to insert")
	}
	if p.query.Type == query.Insert {
		for _, i := range p.query.Inserts {
			if len(i) != len(p.query.Fields) {
				return newError(p.i, "at INSERT INTO: value count doesn't match field count")
			}
		}
	}
	if p.query.Type == query.Select && len(p.query.Fields) != len(p.query.Aliases) {
		return newError(p.i, "fileds and aliases count mismatch")
	}
	return nil
}

func isIdentifier(s string) (bool, bool) {
	if len(s) == 0 {
		return false, false
	}
	u := strings.ToUpper(s)

	if _, ok := reservedWords[u]; ok {
		return false, false
	}

	if s[0] == '-' || (s[0] >= '0' && s[0] <= '9') {
		for i := 1; i < len(s); i++ {
			allowedSymbol := (s[i] >= '0' && s[i] <= '9') || s[i] == '.'
			if !allowedSymbol {
				return false, false
			}
		}
		return false, true
	} else if (s[0] >= 'a' && s[0] <= 'z') ||
		(s[0] >= 'A' && s[0] <= 'Z') ||
		s[0] == '_' {
		for i := 1; i < len(s); i++ {
			isIdentifierSymbol := (s[i] >= 'a' && s[i] <= 'z') ||
				(s[i] >= 'A' && s[i] <= 'Z') ||
				(s[i] >= '0' && s[i] <= '9') ||
				s[i] == '_'
			if !isIdentifierSymbol {
				if s[i] == '(' && s[len(s)-1] == ')' {
					return true, false
				}
				return false, false
			}
		}
		return true, false
	}
	return false, false
}

func isIdentifierOrAsterisk(s string) (bool, bool) {
	if s == "*" {
		return true, false
	}
	return isIdentifier(s)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
