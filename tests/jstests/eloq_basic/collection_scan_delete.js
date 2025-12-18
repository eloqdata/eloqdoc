// Collection Scan + Delete test for MongoDB 4.x
// Usage: mongo collection_scan_delete_test.js

var DB_NAME = "collscan_test_db";
var COLL_NAME = "collscan_delete_col";
var DOC_COUNT = 10000;

var db = db.getSiblingDB(DB_NAME);
var coll = db.getCollection(COLL_NAME);

// Cleanup
coll.drop();

print("Inserting documents...");

var bulk = coll.initializeUnorderedBulkOp();
for (var i = 0; i < DOC_COUNT; i++) {
    bulk.insert({
        _id: i,
        k: i,
        payload: "value_" + i
    });
}
bulk.execute();

print("Inserted documents:", coll.count());

// Ensure only _id index exists
print("Indexes:", tojson(coll.getIndexes()));

//
// Step 1: Explain delete to confirm COLLSCAN
//
print("Explaining delete plan (should be COLLSCAN)...");

var explain = coll.explain("executionStats").remove(
    { k: { $gte: 0 } },
    { hint: { $natural: 1 } }
);

print("Winning plan stage:",
    explain.queryPlanner.winningPlan.inputStage.stage);

print("Total documents examined:",
    explain.executionStats.totalDocsExamined);

print("Total keys examined:",
    explain.executionStats.totalKeysExamined);

assert(explain.queryPlanner.winningPlan.inputStage.stage == "COLLSCAN", "Delete did NOT use COLLSCAN");

//
// Step 2: Perform actual Collection Scan delete
//
print("Running Collection Scan delete...");

var start = new Date();
var result = coll.remove(
    { k: { $gte: 0 } },
    { hint: { $natural: 1 } }
);
var end = new Date();

print("Delete result:", tojson(result));
print("Delete elapsed ms:", end - start);

//
// Step 3: Verify collection is empty
//
var remaining = coll.countDocuments({});
print("Remaining documents:", remaining);
assert.eq(0, remaining, "All documents should be deleted");