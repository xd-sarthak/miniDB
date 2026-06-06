package scan

import "time"

// Scan interface will be implemented by all query scan
type Scan interface {
	// BeforeFirst positions the scan before its first record
	BeforeFirst() error

	// Next moves the scan to the next record. It returns false if there are no more records
	Next() (bool, error)

	// Close closes the scan and releases any resources it holds
	Close()

	// GetInt returns the value of the specified field as an integer
	GetInt(fieldName string) (int, error)

	// GetString returns the value of the specified field as a string
	GetString(fieldName string) (string, error)

	// GetDate returns the value of the specified field as a time.Time
	GetDate(fieldName string) (time.Time, error)

	// GetLong returns the value of the specified field as a long integer
	GetLong(fieldName string) (int64, error)

	// GetShort returns the value of the specified field as a short integer
	GetShort(fieldName string) (int16, error)

	// GetBool returns the value of the specified field as a boolean
	GetBool(fieldName string) (bool, error)

	// HasField checks if the specified field exists in the current record
	HasField(fieldName string) bool

	// GetVal returns the value of the specified field as an interface{}
	GetVal(fieldName string) (any, error)
}