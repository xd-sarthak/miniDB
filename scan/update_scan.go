package scan

import (
	"time"
	"github.com/xd-sarthak/miniDB/records"
)

type UpdateScan interface {
	Scan

	// SetVal sets the value of the specified field to the given value
	SetVal(fieldName string, value any) error
	
	// SetInt sets the value of the specified field to the given integer
	SetInt(fieldName string, value int) error

	// SetString sets the value of the specified field to the given string
	SetString(fieldName string, value string) error

	// SetDate sets the value of the specified field to the given time.Time
	SetDate(fieldName string, value time.Time) error

	// SetLong sets the value of the specified field to the given long integer
	SetLong(fieldName string, value int64) error

	// SetShort sets the value of the specified field to the given short integer
	SetShort(fieldName string, value int16) error

	// SetBool sets the value of the specified field to the given boolean
	SetBool(fieldName string, value bool) error

	// Insert inserts a new record into the scan
	Insert() error

	// Delete deletes the current record from the scan
	Delete() error

	// GetRecordID returns the ID of the current record in the scan
	GetRecordID() *records.ID

	// MoveToRecord moves the scan to the record with the specified ID
	MoveToRecordID(id *records.ID) error
}