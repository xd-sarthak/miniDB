package query

import (
	"fmt"
	"github.com/xd-sarthak/miniDB/records"
	"github.com/xd-sarthak/miniDB/scan"
	"time"
)

// Implements the selection operator
// select basically filters records

// Ensure that SelectScan implements the Scan interface
var _ scan.UpdateScan = (*SelectScan)(nil)

// SelectScan is a scan that returns only those records from the underlying inputScan that satisfy the specified predicate.
type SelectScan struct {
	inputScan scan.Scan
	predicate *Predicate
}

// NewSelectScan creates a new select inputScan with the specified underlying inputScan and predicate.
func NewSelectScan(s scan.Scan, p *Predicate) (*SelectScan, error) {
	return &SelectScan{inputScan: s, predicate: p}, nil
}

// BeforeFirst positions the inputScan before the first record.
// we position the inputScan before the first record because we want to start reading from the beginning of the inputScan
func (ss *SelectScan) BeforeFirst() error {
	return ss.inputScan.BeforeFirst()
}

// Next moves to the next record satisfying the predicate.
// it returns false if there are no more such records
func (ss *SelectScan) Next() (bool, error) {
	for {
		ok, err := ss.inputScan.Next()
		if !ok || err != nil {
			return ok, err
		}
		if ss.predicate == nil {
			return true, nil
		}
		if ss.predicate.IsSatisfied(ss.inputScan) {
			return true, nil
		}
	}
}

// GetInt returns the integer value of the specified field in the current record.
func (ss *SelectScan) GetInt(fieldName string) (int, error) {
	return ss.inputScan.GetInt(fieldName)
}

// GetLong returns the long value of the specified field in the current record.
func (ss *SelectScan) GetLong(fieldName string) (int64, error) {
	return ss.inputScan.GetLong(fieldName)
}

// GetShort returns the short value of the specified field in the current record.
func (ss *SelectScan) GetShort(fieldName string) (int16, error) {
	return ss.inputScan.GetShort(fieldName)
}

// GetString returns the string value of the specified field in the current record.
func (ss *SelectScan) GetString(fieldName string) (string, error) {
	return ss.inputScan.GetString(fieldName)
}

// GetBool returns the boolean value of the specified field in the current record.
func (ss *SelectScan) GetBool(fieldName string) (bool, error) {
	return ss.inputScan.GetBool(fieldName)
}

// GetDate returns the date value of the specified field in the current record.
func (ss *SelectScan) GetDate(fieldName string) (time.Time, error) {
	return ss.inputScan.GetDate(fieldName)
}

// GetVal returns the value of the specified field in the current record.
func (ss *SelectScan) GetVal(fieldName string) (any, error) {
	return ss.inputScan.GetVal(fieldName)
}

// HasField returns true if the underlying scan has the specified field.
func (ss *SelectScan) HasField(fieldName string) bool {
	return ss.inputScan.HasField(fieldName)
}

// Close closes the underlying scan.
func (ss *SelectScan) Close() {
	ss.inputScan.Close()
}

// SetInt sets the integer value of the specified field in the current record.
func (ss *SelectScan) SetInt(fieldName string, val int) error {
	updateScan, ok := ss.inputScan.(scan.UpdateScan)
	if !ok {
		return fmt.Errorf(ErrUpdateNotSupported, ss.inputScan)
	}
	return updateScan.SetInt(fieldName, val)
}

// SetLong sets the long value of the specified field in the current record.
func (ss *SelectScan) SetLong(fieldName string, val int64) error {
	updateScan, ok := ss.inputScan.(scan.UpdateScan)
	if !ok {
		return fmt.Errorf(ErrUpdateNotSupported, ss.inputScan)
	}
	return updateScan.SetLong(fieldName, val)
}

// SetShort sets the short value of the specified field in the current record.
func (ss *SelectScan) SetShort(fieldName string, val int16) error {
	updateScan, ok := ss.inputScan.(scan.UpdateScan)
	if !ok {
		return fmt.Errorf(ErrUpdateNotSupported, ss.inputScan)
	}
	return updateScan.SetShort(fieldName, val)
}

// SetString sets the string value of the specified field in the current record.
func (ss *SelectScan) SetString(fieldName string, val string) error {
	updateScan, ok := ss.inputScan.(scan.UpdateScan)
	if !ok {
		return fmt.Errorf(ErrUpdateNotSupported, ss.inputScan)
	}
	return updateScan.SetString(fieldName, val)
}

// SetBool sets the boolean value of the specified field in the current record.
func (ss *SelectScan) SetBool(fieldName string, val bool) error {
	updateScan, ok := ss.inputScan.(scan.UpdateScan)
	if !ok {
		return fmt.Errorf(ErrUpdateNotSupported, ss.inputScan)
	}
	return updateScan.SetBool(fieldName, val)
}

// SetDate sets the date value of the specified field in the current record.
func (ss *SelectScan) SetDate(fieldName string, val time.Time) error {
	updateScan, ok := ss.inputScan.(scan.UpdateScan)
	if !ok {
		return fmt.Errorf(ErrUpdateNotSupported, ss.inputScan)
	}
	return updateScan.SetDate(fieldName, val)
}

// SetVal sets the value of the specified field in the current record.
func (ss *SelectScan) SetVal(fieldName string, val any) error {
	updateScan, ok := ss.inputScan.(scan.UpdateScan)
	if !ok {
		return fmt.Errorf(ErrUpdateNotSupported, ss.inputScan)
	}
	return updateScan.SetVal(fieldName, val)
}

// Delete deletes the current record from the scan.
func (ss *SelectScan) Delete() error {
	updateScan, ok := ss.inputScan.(scan.UpdateScan)
	if !ok {
		return fmt.Errorf(ErrUpdateNotSupported, ss.inputScan)
	}
	return updateScan.Delete()
}

// Insert inserts a new record somewhere in the scan.
func (ss *SelectScan) Insert() error {
	updateScan, ok := ss.inputScan.(scan.UpdateScan)
	if !ok {
		return fmt.Errorf(ErrUpdateNotSupported, ss.inputScan)
	}
	return updateScan.Insert()
}

// GetRecordID returns the record ID of the current record.
func (ss *SelectScan) GetRecordID() *records.ID {
	updateScan, ok := ss.inputScan.(scan.UpdateScan)
	if !ok {
		panic(fmt.Sprintf(ErrUpdateNotSupported, ss.inputScan))
	}
	return updateScan.GetRecordID()
}

// MoveToRecordID moves the scan to the record with the specified record ID.
func (ss *SelectScan) MoveToRecordID(rid *records.ID) error {
	updateScan, ok := ss.inputScan.(scan.UpdateScan)
	if !ok {
		return fmt.Errorf(ErrUpdateNotSupported, ss.inputScan)
	}
	return updateScan.MoveToRecordID(rid)
}

/*

TableScan
    ↓
SelectScan
    ↓
Output rows


SELECT *
FROM student
WHERE majorid = 10

suppose student table

sid   majorid
---------------
1     20
2     10
3     30
4     10


so we build a query tree from bottom up



first call to Next()

sid   majorid
---------------
1     20  <- current
2     10
3     30
4     10

predicate.IsSatisfied(scan)

evaluates 20 = 10 which is false

so we call Next() again

sid   majorid
---------------
1     20
2     10 <- current
3     30
4     10

predicate.IsSatisfied(scan)

evaluates 10 = 10 which is true

so we return true and the current record is (2, 10)

keep going with Next()



User
 |
 | Next()
 v
SelectScan
 |
 | Next()
 v
TableScan
 |
 | row found
 v
SelectScan
 |
 | Predicate.IsSatisfied()
 v
Predicate
 |
 v
Term
 |
 v
Expression
 |
 v
majorid=10 ?

if true row returned
if false keep scanning


*/