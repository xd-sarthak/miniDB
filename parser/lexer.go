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
			} else {
				// fallback to number if date parsing fails
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

			// check if it's a keyword
			if _, isKeyword := l.keywords[wordLower]; isKeyword {
				l.currentToken = Token{Type: TTWord, StringVal: wordLower}
			} else {
				// otherwise it's an identifier
				l.currentToken = Token{Type: TTWord, StringVal: wordVal}
			}
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
