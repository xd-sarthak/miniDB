package records

// this file defines what kind of values can exist in a record
// and metadata that we must store

/*
let Student(
    id INT,
    name VARCHAR(20),
    active BOOLEAN
) be a table then
 each column is a field

 here 0 -> Integer
	  1 -> Varchar
	  2 -> Boolean
	  ... and so on
*/

// we can use iota to assign integer values to the schema types
type SchemaType int


const (
	Integer  SchemaType = iota
	Varchar
	Boolean
	Long
	Short
	Date
)

// FieldInfo stored meta data about a field

/*
let name VARCHAR(20) be a field then
then DB needs to remember type = Varchar and length = 20
*/
type FieldInfo struct {
	fieldType SchemaType
	length    int
}