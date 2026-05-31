package metadata

type StatInfo struct {
	numBlocks       int // B(T) = number of blocks in the table
	numRecords      int // R(T) = number of records in the table
	distinctValues  map[string]int // V(T, f) = number of distinct values for field f in table T
}

func NewStatInfo(numBlocks int, numRecords int, distinctValues map[string]int) *StatInfo {
	return &StatInfo{
		numBlocks:      numBlocks,
		numRecords:     numRecords,
		distinctValues: distinctValues,
	}
}

// BlocksAccessed returns the number of blocks in the table.
func (s *StatInfo) BlocksAccessed() int {
	return s.numBlocks
}

// RecordsOutput returns the number of records in the table.
func (s *StatInfo) RecordsOutput() int {
	return s.numRecords
}

// DistinctValues returns the number of distinct values for a given field name.
func (s *StatInfo) DistinctValues(fieldName string) int {
	if val, exists := s.distinctValues[fieldName]; exists {
		return val
	}
	return -1 // default value if the field is not found
}