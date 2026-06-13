package parser

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// SyntaxError represents an error encountered during parsing.
type SyntaxError struct {
	Message string
}

func (e *SyntaxError) Error() string {
	return fmt.Sprintf("Syntax error: %s", e.Message)
}

type TokenType int

const (
	TTDelimiter TokenType = iota
	TTNumber
	TTString
	TTWord
	TTBoolean
	TTDate
	TTOperator
	TTEOF
)

type Token struct {
	Type  		TokenType
	StringVal 	string    // for strings and words
	NumVal      int		  // for integers
	BoolVal     bool	  // for booleans
	TimeVal     time.Time // for dates
	Rune        rune      // for delimiters and operators which is basically a unicode character
}

// Lexer is responsible for tokenizing the input string.
type Lexer struct {
	input        string
	position     int
	currentToken Token
	keywords     map[string]struct{}
}

// NewLexer creates a new Lexer instance with the given input string.
func NewLexer(input string) *Lexer {
	l := &Lexer{input: input}
	l.initKeywords()
	_ = l.nextToken() // Initialize the first token
	return l
}


// --------------------------------------
// PUBLIC "match" METHODS
// --------------------------------------


/*

MatchDelimiter checks if the current token is a delimiter
lets take an example

let current token be
Token{
	Type: TTDelimiter,
	Rune: ',',
}

so parser calls lex.MatchDelimiter(',') and it checks if the current token is a delimiter and if its rune is ','
 then it returns true otherwise false

similarly all other match methods work in the same way, they check if the current token is of the expected type and value and return true or false accordingly

*/

// MatchDelimiter checks if the current token is a delimiter and matches the given rune.
func (l *Lexer) MatchDelimiter(delim rune) bool {
	return l.currentToken.Type == TTDelimiter && l.currentToken.Rune == delim
}

// MatchIntConstant returns true if the current token is an integer.
func (l *Lexer) MatchIntConstant() bool {
	return l.currentToken.Type == TTNumber
}

// MatchStringConstant returns true if the current token is a string constant.
func (l *Lexer) MatchStringConstant() bool {
	return l.currentToken.Type == TTString
}

// MatchKeyword returns true if the current token is the specified keyword.
func (l *Lexer) MatchKeyword(w string) bool {
	return (l.currentToken.Type == TTWord && l.currentToken.StringVal == strings.ToLower(w))
}


/*

SELECT name FROM student
here SELECT, FROM are keywords
name, student are identifiers (non-keyword words)
both are TTWord

MatchId checks first if token even is a TTWord
if it is not a TTWord then it returns false

then it looks up the string value of the token in the keywords map to see if it is a keyword
if it is a keyword then it returns false because we are looking for identifiers (non-keyword words)
if it is not a keyword then it returns true because it is a valid identifier

*/

// MatchId returns true if the current token is a legal identifier (non-keyword).
func (l *Lexer) MatchId() bool {
	if l.currentToken.Type != TTWord {
		return false
	}
	_, isKeyword := l.keywords[l.currentToken.StringVal]
	return !isKeyword
}

// MatchBooleanConstant returns true if the current token is a boolean (true/false).
func (l *Lexer) MatchBooleanConstant() bool {
	return l.currentToken.Type == TTBoolean
}

// MatchDateConstant returns true if the current token is a date token.
func (l *Lexer) MatchDateConstant() bool {
	return l.currentToken.Type == TTDate
}

// MatchOperator returns true if the current token is an operator (e.g. "=", ">=", etc.).
func (l *Lexer) MatchOperator(op string) bool {
	return l.currentToken.Type == TTOperator && l.currentToken.StringVal == op
}

// ---------------------------------------
// PUBLIC "eat" METHODS
// ---------------------------------------



func (l *Lexer) EatDelimiter(delim rune) error {
	if !l.MatchDelimiter(delim) {
		return &SyntaxError{Message: fmt.Sprintf("Expected delimiter '%c'", delim)}
	}
	return l.nextToken()
}


func (l *Lexer) EatIntConstant() (int, error) {
	if !l.MatchIntConstant() {
		return 0, &SyntaxError{Message: "expected integer constant"}
	}
	val := l.currentToken.NumVal
	if err := l.nextToken(); err != nil {
		return 0, err
	}
	return val, nil
}

func (l *Lexer) EatStringConstant() (string, error) {
	if !l.MatchStringConstant() {
		return "", &SyntaxError{Message: "expected string constant"}
	}
	val := l.currentToken.StringVal
	if err := l.nextToken(); err != nil {
		return "", err
	}
	return val, nil
}

func (l *Lexer) EatKeyword(w string) error {
	if !l.MatchKeyword(w) {
		return &SyntaxError{Message: fmt.Sprintf("expected keyword '%s'", w)}
	}
	return l.nextToken()
}

func (l *Lexer) EatId() (string, error) {
	if !l.MatchId() {
		return "", &SyntaxError{Message: "expected identifier"}
	}
	val := l.currentToken.StringVal
	if err := l.nextToken(); err != nil {
		return "", err
	}
	return val, nil
}

func (l *Lexer) EatBooleanConstant() (bool, error) {
	if !l.MatchBooleanConstant() {
		return false, &SyntaxError{Message: "expected boolean constant (true/false)"}
	}
	val := l.currentToken.BoolVal
	if err := l.nextToken(); err != nil {
		return false, err
	}
	return val, nil
}

func (l *Lexer) EatDateConstant() (time.Time, error) {
	if !l.MatchDateConstant() {
		return time.Time{}, &SyntaxError{Message: "expected date constant"}
	}
	val := l.currentToken.TimeVal
	if err := l.nextToken(); err != nil {
		return time.Time{}, err
	}
	return val, nil
}

// EatOperator checks if the current token is the specified operator.
// If so, it advances the lexer; otherwise returns an error.
func (l *Lexer) EatOperator(op string) error {
	if !l.MatchOperator(op) {
		return &SyntaxError{Message: fmt.Sprintf("expected operator '%s'", op)}
	}
	return l.nextToken()
}

//---------------------------------
// PRIVATE METHODS
//---------------------------------


/*

this func build lexer's keyword dictionary

l.keywords = map[string]struct{}{
    "select": {},
    "from": {},
    "where": {},
    "and": {},
    "insert": {},
    ...
}

*/

// initKeywords initializes the set of SQL keywords for quick lookup.
func (l *Lexer) initKeywords() {
	kwList := []string{
		"select", "from", "where", "and",
		"insert", "into", "values", "delete", "update", "set",
		"create", "table", "int", "varchar", "view", "as", "index", "on",
	}
	l.keywords = make(map[string]struct{}, len(kwList)) /// set in go is implemented as a map with empty struct values to save memory
	// loop through the keywords and converts to lowercase to maake SQL case-insensitive and adds to the keywords map
	for _, kw := range kwList {
		l.keywords[strings.ToLower(kw)] = struct{}{}
	}
}

/*

the job of nexttoken is

	Current character position
            ↓
      Read characters
            ↓
      Build next token
            ↓
	Store it in currentToken
            ↓
	Move position forward

so step by step it does the following:

1. we skip whitespace
2. we do end of input checks and if cursor reaches end of input then we retunn an EOF token
3. read current rune, current character and how many bytes it occupies
eg. r = 'A width  = 1
eg r = '₹' width = 3

4. check operators first, < > = ! 
5. check string literals
6. check delimiters
7. check numbers
8. check words (keywords and identifiers)
9. if we get here then it is an invalid character and we return an error



1. Skip whitespace

2. End of input?
      ↓
      EOF

3. Read current character

4. What starts here?

   Operator?  -> scan operator
   String?    -> scan string
   Delimiter? -> delimiter token
   Number?    -> scan number/date
   Letter?    -> scan word/boolean

5. Build Token

6. Store in currentToken

7. Return


*/



func (l *Lexer) nextToken() error {
	// we ignore whitespace between tokens, so we skip them first
	l.skipWhitespace()
	// EOF check
	if l.position >= len(l.input) {
		l.currentToken = Token{Type: TTEOF}
		return nil // EOF token
	}

	// decode the current characcter
	r, width := utf8.DecodeRuneInString(l.input[l.position:])

	// check for multi/single-char operators first
	// if > we treat as operator
	if isOperatorStart(r) {
		// looks for operators like =, >, <, >=, <=, !=, <>
		op, err := l.scanOperator()
		if err != nil {
			return err
		}
		l.currentToken = Token{Type: TTOperator, StringVal: op}
		return nil
	}
	
	// if not operator
	switch {
		// looks for string literals like 'hello world'
		// \ because we are looking for '
		case r == '\'':
			str, err := l.scanString() // consumes string and removes quotes
			if err != nil {
				return err
			}
			l.currentToken = Token{Type: TTString, StringVal: str}
			return nil
		case isDelimiter(r):
			l.currentToken = Token{Type: TTDelimiter, Rune: r}
			l.position += width
			return nil
		case unicode.IsDigit(r):
			start := l.position
			for l.position < len(l.input) {
				r, w := utf8.DecodeRuneInString(l.input[l.position:])
				if !unicode.IsDigit(r) && r != '-' && r != ':' && r != ' ' {
					break
				}
				l.position += w
			}
			tokenStr := l.input[start:l.position]
			if t,err := parseDate(tokenStr); err == nil {
				l.currentToken = Token{Type: TTDate, TimeVal: t}
				return nil
			} else {
				// fallback to number if date parsing fails; strip any
				// trailing/embedded whitespace consumed by the date scan.
				tokenStr = strings.ReplaceAll(tokenStr, " ", "")
				if num, err := strconv.Atoi(tokenStr); err == nil {
					l.currentToken = Token{Type: TTNumber, NumVal: num}
					return nil
				} else {
					return &SyntaxError{Message: fmt.Sprintf("invalid number/date format: '%s'", tokenStr)}
				}
			}
		
		case unicode.IsLetter(r) || r == '_':
			wordVal := l.scanWord()
			wordLower := strings.ToLower(wordVal)

			// check if it's a boolean constant
			if wordLower == "true" {
				l.currentToken = Token{Type: TTBoolean, BoolVal: true}
				return nil
			} else if wordLower == "false" {
				l.currentToken = Token{Type: TTBoolean, BoolVal: false}
				return nil
			}

			// Treat keywords and identifiers uniformly as lowercase words so
			// that table/field names are case-insensitive (matching dropdb).
			l.currentToken = Token{Type: TTWord, StringVal: wordLower}
			return nil
	}

	return &SyntaxError{Message: fmt.Sprintf("unexpected character '%c'", r)}
}


// scanOperator checks for either single- or multi-character operators
// like '=', '>', '<', '>=', '<=', '!=', '<>', etc.
func (l *Lexer) scanOperator() (string, error) {
	r, width := utf8.DecodeRuneInString(l.input[l.position:])
	l.position += width

	if l.position < len(l.input) {
		// Look ahead to see if next char is '=' or '>'
		r2, w2 := utf8.DecodeRuneInString(l.input[l.position:])

		// Combine if it forms one of the multi-char operators
		if (r == '>' && r2 == '=') || (r == '<' && r2 == '=') ||
			(r == '!' && r2 == '=') || (r == '<' && r2 == '>') {
			// e.g. ">=", "<=", "!=", "<>"
			l.position += w2
			return string([]rune{r, r2}), nil
		}
	}
	// If no multi-char operator, return single char as operator
	return string(r), nil
}

// scanString scans a single-quoted string literal.
// Returns the string value (without quotes), or an error if unterminated.
func (l *Lexer) scanString() (string, error) {
	l.position++ // consume the quote
	var sb strings.Builder

	for l.position < len(l.input) {
		r, width := utf8.DecodeRuneInString(l.input[l.position:])
		if r == '\'' {
			// Found the closing quote
			l.position += width
			return sb.String(), nil
		}
		sb.WriteRune(r)
		l.position += width
	}
	return "", &SyntaxError{Message: "unterminated string constant"}
}

// scanWord scans an identifier-like token (letters, digits, underscores).
func (l *Lexer) scanWord() string {
	start := l.position
	for l.position < len(l.input) {
		r, width := utf8.DecodeRuneInString(l.input[l.position:])
		if !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_') {
			break
		}
		l.position += width
	}
	return l.input[start:l.position]
}

// skipWhitespace advances over any sequence of whitespace.
func (l *Lexer) skipWhitespace() {
	for l.position < len(l.input) {
		r, width := utf8.DecodeRuneInString(l.input[l.position:])
		if !unicode.IsSpace(r) {
			break
		}
		l.position += width
	}
}


// parseDate attempts to parse s as a date in one of our accepted formats.
// Returns time.Time if successful, or an error if none of the formats matched.
func parseDate(s string) (time.Time, error) {
	// Accept "YYYY-MM-DD" or "YYYY-MM-DD HH:MM:SS"
	layouts := []string{
		"2006-01-02",
		"2006-01-02 15:04:05",
	}
	for _, layout := range layouts {
		t, err := time.Parse(layout, s)
		if err == nil {
			return t, nil
		}
	}
	return time.Time{}, &SyntaxError{
		Message: fmt.Sprintf("invalid date format: '%s'", s),
	}
}

// isOperatorStart returns true if this rune starts an operator (e.g. <, >, =, !).
func isOperatorStart(r rune) bool {
	switch r {
	case '<', '>', '=', '!':
		return true
	default:
		return false
	}
}

// isDelimiter checks if a rune is treated as a single-character delimiter.
// (Operators are handled separately, in isOperatorStart/scanOperator.)
func isDelimiter(r rune) bool {
	// e.g. commas, parentheses, semicolons, plus, minus, period...
	// We deliberately *exclude* <, >, =, ! so we can handle multi-char operators.
	delimiters := []rune{',', '(', ')', '.', ';', '+', '-'}
	for _, d := range delimiters {
		if r == d {
			return true
		}
	}
	return false
}

/*

let the query be

SELECT name,age
FROM student
WHERE age >= 18 AND active = true

step 0: create lexer

we do lex := NewLexer(sql)

now the state is
input: "SELECT name,age FROM student WHERE age >= 18 AND active = true"
position: 0
currentToken = empty

then l.initKeywords() is called to initialize the keywords map

then we call l.nextToken() to read the first token


STEP 1: read first token

so current state is:
SELECT name, age ...
^
position

since no whitespace or EOF skip

now decode rune as r = 's'

operator? no
string? no
delimiter? no
number? no
letter? yes

so we call scanWord() which consumes SELECT

now position moves:
SELECT name, age
      ^

so we store as currentToken = Token{Type: TTWord, StringVal: "select"}

we consume it and move to next token

STEP 2: read next token

repeat same steps and we get currentToken = Token{Type: TTWord, StringVal: "name"}

STEP 3: read next token

we get currentToken = Token{Type: TTDelimiter, Rune: ','}

STEP 4: read next token

we get currentToken = Token{Type: TTWord, StringVal: "age"}

STEP 5: read next token

we get currentToken = Token{Type: TTWord, StringVal: "from"}

and so on until we reach the end of the input and get currentToken = Token{Type: TTEOF}

SELECT
↓
WORD(select)

name
↓
WORD(name)

,
↓
DELIMITER(',')

age
↓
WORD(age)

FROM
↓
WORD(from)

student
↓
WORD(student)

WHERE
↓
WORD(where)

age
↓
WORD(age)

>=
↓
OPERATOR(">=")

20
↓
NUMBER(20)

AND
↓
WORD(and)

active
↓
WORD(active)

=
↓
OPERATOR("=")

true
↓
BOOLEAN(true)

EOF
↓
TTEOF






*/
