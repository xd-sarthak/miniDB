package btree

import (
	"fmt"
	"github.com/xd-sarthak/miniDB/buffer"
	"github.com/xd-sarthak/miniDB/file"
	"github.com/xd-sarthak/miniDB/index"
	"github.com/xd-sarthak/miniDB/log"
	"github.com/xd-sarthak/miniDB/records"
	"github.com/xd-sarthak/miniDB/transaction"
	"github.com/xd-sarthak/miniDB/transaction/concurrency"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math"
	"os"
	"testing"
)

func setupBTreeIndexTest(t *testing.T) (index.Index, func()) {
	dbDir := t.TempDir()

	fm, err := file.NewManager(dbDir, 800)
	require.NoError(t, err)

	lm, err := log.NewManager(fm, "logfile")
	require.NoError(t, err)

	bm := buffer.NewManager(fm, lm, 256)

	transaction, err := transaction.NewTransaction(fm, lm, bm, concurrency.NewLockTable())
	require.NoError(t, err)

	schema := records.NewSchema()
	schema.AddIntField(index.Blockfield)
	schema.AddIntField(index.IDField)
	schema.AddStringField(index.DataValueField, 20)

	layout := records.NewLayout(schema)
	indexName := "test_btree_index"
	btreeIndex, err := NewIndex(transaction, indexName, layout)
	require.NoError(t, err)

	cleanup := func() {
		btreeIndex.Close()
		if err := transaction.Commit(); err != nil {
			t.Error(err)
		}
		if err := os.RemoveAll(dbDir); err != nil {
			t.Error(err)
		}
	}

	return btreeIndex, cleanup
}

func TestBTreeIndex_BeforeFirst(t *testing.T) {
	btreeIndex, cleanup := setupBTreeIndexTest(t)
	defer cleanup()

	err := btreeIndex.BeforeFirst("test_key")
	require.NoError(t, err)
	assert.NotNil(t, btreeIndex.(*Index).leaf)
}

func TestBTreeIndex_Insert_And_Search(t *testing.T) {
	btreeIndex, cleanup := setupBTreeIndexTest(t)
	defer cleanup()

	// Insert multiple records to test both leaf and directory operations
	testRecords := []*records.ID{
		records.NewID(1, 1),
		records.NewID(1, 2),
		records.NewID(1, 3),
	}

	// Insert records
	for _, rid := range testRecords {
		err := btreeIndex.Insert("test_key", rid)
		require.NoError(t, err)
	}

	// Search and verify
	err := btreeIndex.BeforeFirst("test_key")
	require.NoError(t, err)

	// Verify all records are found
	foundCount := 0
	for {
		hasNext, err := btreeIndex.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		rid, err := btreeIndex.GetDataRecordID()
		require.NoError(t, err)
		assert.Contains(t, testRecords, rid)
		foundCount++
	}

	assert.Equal(t, len(testRecords), foundCount)
}

func TestBTreeIndex_Delete(t *testing.T) {
	btreeIndex, cleanup := setupBTreeIndexTest(t)
	defer cleanup()

	// Insert and then delete a record
	dataRecordID := records.NewID(1, 1)
	err := btreeIndex.Insert("test_key", dataRecordID)
	require.NoError(t, err)

	err = btreeIndex.Delete("test_key", dataRecordID)
	require.NoError(t, err)

	// Verify deletion
	err = btreeIndex.BeforeFirst("test_key")
	require.NoError(t, err)

	hasNext, err := btreeIndex.Next()
	require.NoError(t, err)
	assert.False(t, hasNext)
}

func TestBTreeIndex_Next_NoResults(t *testing.T) {
	btreeIndex, cleanup := setupBTreeIndexTest(t)
	defer cleanup()

	err := btreeIndex.BeforeFirst("nonexistent_key")
	require.NoError(t, err)

	hasNext, err := btreeIndex.Next()
	require.NoError(t, err)
	assert.False(t, hasNext)
}

func TestBTreeIndex_SearchCost(t *testing.T) {
	btreeIndex, cleanup := setupBTreeIndexTest(t)
	defer cleanup()

	numBlocks := 1000
	recordsPerBlock := 10

	cost := btreeIndex.(*Index).SearchCost(numBlocks, recordsPerBlock)
	expectedCost := 1 + int(math.Log(float64(numBlocks))/math.Log(float64(recordsPerBlock)))
	assert.Equal(t, expectedCost, cost)
}

func TestBTreeIndex_MultipleValues(t *testing.T) {
	btreeIndex, cleanup := setupBTreeIndexTest(t)
	defer cleanup()

	// Insert records with different keys
	testData := map[string]*records.ID{
		"key1": records.NewID(1, 1),
		"key2": records.NewID(1, 2),
		"key3": records.NewID(1, 3),
	}

	for key, rid := range testData {
		err := btreeIndex.Insert(key, rid)
		require.NoError(t, err)
	}

	// Verify each key can be found
	for key, expectedRID := range testData {
		err := btreeIndex.BeforeFirst(key)
		require.NoError(t, err)

		hasNext, err := btreeIndex.Next()
		require.NoError(t, err)
		assert.True(t, hasNext)

		rid, err := btreeIndex.GetDataRecordID()
		require.NoError(t, err)
		assert.Equal(t, expectedRID, rid)
	}
}

func TestBTreeIndex_InsertDuplicateKeys(t *testing.T) {
	btreeIndex, cleanup := setupBTreeIndexTest(t)
	defer cleanup()

	key := "same_key"
	rid1 := records.NewID(1, 1)
	rid2 := records.NewID(1, 2)

	// Insert two records with the same key
	err := btreeIndex.Insert(key, rid1)
	require.NoError(t, err)
	err = btreeIndex.Insert(key, rid2)
	require.NoError(t, err)

	// Verify both records can be found
	err = btreeIndex.BeforeFirst(key)
	require.NoError(t, err)

	foundRIDs := make([]*records.ID, 0)
	for {
		hasNext, err := btreeIndex.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		rid, err := btreeIndex.GetDataRecordID()
		require.NoError(t, err)
		foundRIDs = append(foundRIDs, rid)
	}

	assert.Len(t, foundRIDs, 2)
	assert.Contains(t, foundRIDs, rid1)
	assert.Contains(t, foundRIDs, rid2)
}

// Warning: This test is slow
func TestBTreeIndex_NodeSplits(t *testing.T) {
	btreeIndex, cleanup := setupBTreeIndexTest(t)
	defer cleanup()

	// Insert many records to force leaf node splits
	numRecords := 100
	keys := make([]string, numRecords)
	recordIDs := make([]*records.ID, numRecords)

	for i := 0; i < numRecords; i++ {
		keys[i] = fmt.Sprintf("key_%d", i)
		recordIDs[i] = records.NewID(1, i)
		err := btreeIndex.Insert(keys[i], recordIDs[i])
		require.NoError(t, err)
	}

	// Verify all records can still be found
	for i := 0; i < numRecords; i++ {
		err := btreeIndex.BeforeFirst(keys[i])
		require.NoError(t, err)

		hasNext, err := btreeIndex.Next()
		require.NoError(t, err)
		assert.True(t, hasNext)

		rid, err := btreeIndex.GetDataRecordID()
		require.NoError(t, err)
		assert.Equal(t, recordIDs[i], rid)
	}
}

// Warning: This test is slow
func TestBTreeIndex_DirectorySplits(t *testing.T) {
	btreeIndex, cleanup := setupBTreeIndexTest(t)
	defer cleanup()

	// Insert records with same key to force directory splits
	const numRecords = 300
	key := "same_key"
	recordIDs := make([]*records.ID, numRecords)

	for i := 0; i < numRecords; i++ {
		recordIDs[i] = records.NewID(1, i)
		err := btreeIndex.Insert(key, recordIDs[i])
		require.NoError(t, err)
	}

	// Verify all records are found
	err := btreeIndex.BeforeFirst(key)
	require.NoError(t, err)

	foundCount := 0
	for {
		hasNext, err := btreeIndex.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		rid, err := btreeIndex.GetDataRecordID()
		require.NoError(t, err)
		assert.Contains(t, recordIDs, rid)
		foundCount++
	}

	assert.Equal(t, numRecords, foundCount)
}
