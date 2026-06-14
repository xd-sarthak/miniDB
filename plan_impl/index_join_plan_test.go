package plan_impl

import (
	"testing"

	"github.com/xd-sarthak/miniDB/buffer"
	"github.com/xd-sarthak/miniDB/file"
	"github.com/xd-sarthak/miniDB/index"
	"github.com/xd-sarthak/miniDB/index/hash"
	"github.com/xd-sarthak/miniDB/log"
	"github.com/xd-sarthak/miniDB/metadata"
	"github.com/xd-sarthak/miniDB/records"
	"github.com/xd-sarthak/miniDB/tablescan"
	"github.com/xd-sarthak/miniDB/transaction"
	"github.com/xd-sarthak/miniDB/transaction/concurrency"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
)

type indexJoinPlanTestSetup struct {
	transaction  *transaction.Transaction
	empScan      *tablescan.TableScan
	deptScan     *tablescan.TableScan
	idx          index.Index
	indexInfo    *metadata.IndexInfo
	empMetadata  *metadata.Manager
	deptMetadata *metadata.Manager
	cleanup      func()
}

func setupIndexJoinPlanTest(t *testing.T) *indexJoinPlanTestSetup {
	dbDir := t.TempDir()
	fm, err := file.NewManager(dbDir, 800)
	require.NoError(t, err)

	lm, err := log.NewManager(fm, "logfile")
	require.NoError(t, err)

	bm := buffer.NewManager(fm, lm, 10)
	transaction, _ := transaction.NewTransaction(fm, lm, bm, concurrency.NewLockTable())

	// Department schema is needed to describe the indexed field to IndexInfo.
	deptSchema := records.NewSchema()
	deptSchema.AddIntField("dept_id") // Join field
	deptSchema.AddStringField("dept_name", 20)
	deptSchema.AddIntField("budget")

	// Create index schema and layout (the index uses one consistent layout for
	// both writes and reads).
	idxSchema := records.NewSchema()
	idxSchema.AddIntField(index.Blockfield)
	idxSchema.AddIntField(index.IDField)
	idxSchema.AddIntField(index.DataValueField)
	idxLayout := records.NewLayout(idxSchema)

	// Register the tables in the catalog FIRST, then read the layout back from
	// the catalog and use it to write the data. Writing and reading through the
	// same catalog layout is what the real database does; building a separate
	// layout could assign different field offsets and corrupt reads.
	empMetadata := createTableMetadataWithSchema(t, transaction, "employee", map[string]interface{}{
		"emp_id":  0,
		"name":    "string",
		"dept_id": 0,
	})
	deptMetadata := createTableMetadataWithSchema(t, transaction, "department", map[string]interface{}{
		"dept_id":   0,
		"dept_name": "string",
		"budget":    0,
	})

	empLayout, err := empMetadata.GetLayout("employee", transaction)
	require.NoError(t, err)
	deptLayout, err := deptMetadata.GetLayout("department", transaction)
	require.NoError(t, err)

	// Create table scans
	empScan, err := tablescan.NewTableScan(transaction, "employee", empLayout)
	require.NoError(t, err)

	deptScan, err := tablescan.NewTableScan(transaction, "department", deptLayout)
	require.NoError(t, err)

	// Create index
	idx := hash.NewIndex(transaction, "dept_idx", idxLayout)

	// Insert test data into departments
	deptData := []struct {
		deptID int
		name   string
		budget int
	}{
		{1, "Marketing", 100000},
		{2, "Engineering", 200000},
		{3, "Sales", 150000},
	}

	for _, d := range deptData {
		require.NoError(t, deptScan.Insert())
		require.NoError(t, deptScan.SetInt("dept_id", d.deptID))
		require.NoError(t, deptScan.SetString("dept_name", d.name))
		require.NoError(t, deptScan.SetInt("budget", d.budget))

		rid := deptScan.GetRecordID()
		require.NoError(t, idx.Insert(d.deptID, rid))
	}

	// Insert test data into employees
	empData := []struct {
		empID  int
		name   string
		deptID int
	}{
		{1, "Alice", 1},
		{2, "Bob", 2},
		{3, "Carol", 2},
		{4, "Dave", 3},
		{5, "Eve", 1},
	}

	for _, e := range empData {
		require.NoError(t, empScan.Insert())
		require.NoError(t, empScan.SetInt("emp_id", e.empID))
		require.NoError(t, empScan.SetString("name", e.name))
		require.NoError(t, empScan.SetInt("dept_id", e.deptID))
	}

	// Create StatInfo and IndexInfo
	statInfo := metadata.NewStatInfo(3, 3, map[string]int{
		"dept_id":   3,
		"dept_name": 3,
		"budget":    3,
	})

	indexInfo := metadata.NewIndexInfo(
		"dept_idx",
		"dept_id",
		deptSchema,
		transaction,
		statInfo,
	)

	cleanup := func() {
		empScan.Close()
		deptScan.Close()
		idx.Close()
		if err := transaction.Commit(); err != nil {
			t.Error(err)
		}
		if err := os.RemoveAll(dbDir); err != nil {
			t.Error(err)
		}
	}

	return &indexJoinPlanTestSetup{
		transaction:  transaction,
		empScan:      empScan,
		deptScan:     deptScan,
		idx:          idx,
		indexInfo:    indexInfo,
		empMetadata:  empMetadata,
		deptMetadata: deptMetadata,
		cleanup:      cleanup,
	}
}

func TestIndexJoinPlan_Basic(t *testing.T) {
	setup := setupIndexJoinPlanTest(t)
	defer setup.cleanup()

	// Create plans for both tables
	empPlan, err := NewTablePlan(setup.transaction, "employee", setup.empMetadata)
	require.NoError(t, err)

	deptPlan, err := NewTablePlan(setup.transaction, "department", setup.deptMetadata)
	require.NoError(t, err)

	// Create index join plan
	ijp := NewIndexJoinPlan(empPlan, deptPlan, *setup.indexInfo, "dept_id")

	// Test scan execution
	scan, err := ijp.Open()
	require.NoError(t, err)
	defer scan.Close()

	matchCount := 0
	require.NoError(t, scan.BeforeFirst())
	for {
		hasNext, err := scan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		// Verify employee data
		name, err := scan.GetString("name")
		require.NoError(t, err)

		// Verify department data
		deptName, err := scan.GetString("dept_name")
		require.NoError(t, err)
		budget, err := scan.GetInt("budget")
		require.NoError(t, err)

		// Verify join results
		switch name {
		case "Alice", "Eve":
			assert.Equal(t, "Marketing", deptName)
			assert.Equal(t, 100000, budget)
		case "Bob", "Carol":
			assert.Equal(t, "Engineering", deptName)
			assert.Equal(t, 200000, budget)
		case "Dave":
			assert.Equal(t, "Sales", deptName)
			assert.Equal(t, 150000, budget)
		default:
			t.Errorf("Unexpected employee: %s", name)
		}

		matchCount++
	}
	assert.Equal(t, 5, matchCount)

	// Test plan statistics
	assert.True(t, ijp.BlocksAccessed() > 0)
	assert.Equal(t, 5, ijp.RecordsOutput())
	assert.Equal(t, 5, ijp.DistinctValues("emp_id"))
	assert.Equal(t, 3, ijp.DistinctValues("dept_id"))
}

func TestIndexJoinPlan_NoMatches(t *testing.T) {
	setup := setupIndexJoinPlanTest(t)
	defer setup.cleanup()

	// Insert employee with non-existent department
	require.NoError(t, setup.empScan.Insert())
	require.NoError(t, setup.empScan.SetInt("emp_id", 99))
	require.NoError(t, setup.empScan.SetString("name", "NoMatch"))
	require.NoError(t, setup.empScan.SetInt("dept_id", 999))

	empPlan, err := NewTablePlan(setup.transaction, "employee", setup.empMetadata)
	require.NoError(t, err)

	deptPlan, err := NewTablePlan(setup.transaction, "department", setup.deptMetadata)
	require.NoError(t, err)

	ijp := NewIndexJoinPlan(empPlan, deptPlan, *setup.indexInfo, "dept_id")

	scan, err := ijp.Open()
	require.NoError(t, err)
	defer scan.Close()

	// Count all matches to verify the employee with non-existent department is skipped
	matchCount := 0
	require.NoError(t, scan.BeforeFirst())
	for {
		hasNext, err := scan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		name, err := scan.GetString("name")
		require.NoError(t, err)
		assert.NotEqual(t, "NoMatch", name)
		matchCount++
	}

	assert.Equal(t, 5, matchCount) // Only original matches, excluding NoMatch
}

func TestIndexJoinPlan_MultipleMatches(t *testing.T) {
	setup := setupIndexJoinPlanTest(t)
	defer setup.cleanup()

	empPlan, err := NewTablePlan(setup.transaction, "employee", setup.empMetadata)
	require.NoError(t, err)

	deptPlan, err := NewTablePlan(setup.transaction, "department", setup.deptMetadata)
	require.NoError(t, err)

	ijp := NewIndexJoinPlan(empPlan, deptPlan, *setup.indexInfo, "dept_id")

	scan, err := ijp.Open()
	require.NoError(t, err)
	defer scan.Close()

	// Count employees in each department
	deptCounts := make(map[string]int)
	require.NoError(t, scan.BeforeFirst())
	for {
		hasNext, err := scan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		deptName, err := scan.GetString("dept_name")
		require.NoError(t, err)
		deptCounts[deptName]++
	}

	// Verify counts
	assert.Equal(t, 2, deptCounts["Marketing"])
	assert.Equal(t, 2, deptCounts["Engineering"])
	assert.Equal(t, 1, deptCounts["Sales"])
}

func TestIndexJoinPlan_Schema(t *testing.T) {
	setup := setupIndexJoinPlanTest(t)
	defer setup.cleanup()

	empPlan, err := NewTablePlan(setup.transaction, "employee", setup.empMetadata)
	require.NoError(t, err)

	deptPlan, err := NewTablePlan(setup.transaction, "department", setup.deptMetadata)
	require.NoError(t, err)

	ijp := NewIndexJoinPlan(empPlan, deptPlan, *setup.indexInfo, "dept_id")

	// Test schema contains fields from both tables
	schema := ijp.Schema()
	assert.True(t, schema.HasField("emp_id"))
	assert.True(t, schema.HasField("name"))
	assert.True(t, schema.HasField("dept_id"))
	assert.True(t, schema.HasField("dept_name"))
	assert.True(t, schema.HasField("budget"))
}
