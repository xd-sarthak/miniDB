package records

// Schema represents record schema of the table
// it contains list of fields and their metadata
type Schema struct {
	fields []string // in order of the fields in the table (ordered list for order)
	info   map[string]FieldInfo // metadata about each field (hashmap for fast access)
}

/*

let CREATE TABLE Student (
    id INT,
    name VARCHAR(20),
    active BOOLEAN
)

becomes 

Schema {
    fields = [id, name, active]

    info = {
        id     -> INTEGER
        name   -> VARCHAR(20)
        active -> BOOLEAN
    }
}
*/

// NewSchema creates a new empty schema
func NewSchema() *Schema {
	return &Schema{
		fields: make([]string, 0),
		info:   make(map[string]FieldInfo),
	}
}

// AddField adds a new field to the schema with its metadata
// If the field type is not character type, length is ignored and can be set to 0
func (s *Schema) AddField(name string, fieldType SchemaType, length int) {
	s.fields = append(s.fields, name)
	s.info[name] = FieldInfo{
		fieldType: fieldType,
		length:    length,
	}
}

// AddIntField adds an integer field to the schema.
func (s *Schema) AddIntField(fieldName string) {
	s.AddField(fieldName, Integer, 0)
}

// AddStringField adds a string field to the schema.
func (s *Schema) AddStringField(fieldName string, length int) {
	s.AddField(fieldName, Varchar, length)
}

// AddBoolField adds a boolean field to the schema.
func (s *Schema) AddBoolField(fieldName string) {
	s.AddField(fieldName, Boolean, 0)
}

// AddLongField adds a long field to the schema.
func (s *Schema) AddLongField(fieldName string) {
	s.AddField(fieldName, Long, 0)
}

// AddShortField adds a short field to the schema.
func (s *Schema) AddShortField(fieldName string) {
	s.AddField(fieldName, Short, 0)
}

// AddDateField adds a date field to the schema.
func (s *Schema) AddDateField(fieldName string) {
	s.AddField(fieldName, Date, 0)
}

// Add adds a field to the schema having the sme metadaata
// as corresponding field in the specified schema
// basically copies ONE field definition from other schema to this schema
// useful for projections, joins, derived tables
func (s *Schema) Add(fieldName string, other *Schema) {
	info := other.info[fieldName]
	s.AddField(fieldName, info.fieldType, info.length)
}

// AddAll adds all the fields in the specified schema to the current schema.
// useful for SELECT * queries, joins, derived tables
func (s *Schema) AddAll(other *Schema) {
	for _, field := range other.fields {
		s.Add(field, other)
	}
}

// Fields returns the names of all the fields in the schema.
func (s *Schema) Fields() []string {
	return s.fields
}

// HasField returns true if the schema contains a field with the specified name.
func (s *Schema) HasField(fieldName string) bool {
	_, ok := s.info[fieldName]
	return ok
}

// Type returns the type of the field with the specified name.
func (s *Schema) Type(fieldName string) SchemaType {
	return s.info[fieldName].fieldType
}

// Length returns the length of the field with the specified name.
func (s *Schema) Length(fieldName string) int {
	return s.info[fieldName].length
}