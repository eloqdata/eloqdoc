// ===============================================
// TTL Functional Test with Random Expiration
// ===============================================

function getParameterValue(paramName) {
    const res = db.adminCommand({ getParameter: 1, [paramName]: 1 });
    if (!res.ok) throw new Error("Failed to get parameter: " + paramName);
    return res[paramName];
}

function setParameterValue(paramName, value) {
    const res = db.adminCommand({ setParameter: 1, [paramName]: value });
    if (!res.ok) throw new Error("Failed to set parameter: " + paramName);
}

// -----------------------------
// Test Setup
// -----------------------------

const dbName = "ttl_test_db";
const collName = "ttl_random_test_coll";

// Number of documents
const docCount = 10000;

// Random expiration range: [1, 10] seconds
const minTTL = 1;
const maxTTL = 10;

print("== TTL Functional Test: Random Expiration ==");

// Select database
const dbTest = db.getSiblingDB(dbName);

// Clean up
dbTest[collName].drop();

// Save original TTL monitor interval
const originalTTL = getParameterValue("ttlMonitorSleepSecs");
print(`Original ttlMonitorSleepSecs: ${originalTTL}`);

// Set ttlMonitorSleepSecs to 1 sec for faster test
print("Setting ttlMonitorSleepSecs to 1...");
setParameterValue("ttlMonitorSleepSecs", 1);

// Create collection and TTL index
dbTest.createCollection(collName);
dbTest[collName].createIndex(
    { expireAt: 1 },
    { expireAfterSeconds: 0 }
);

print("TTL index created.");

// -----------------------------
// Insert random-expiring documents
// -----------------------------

print(`Inserting ${docCount} documents with random expiration...`);

let bulk = dbTest[collName].initializeUnorderedBulkOp();
const now = Date.now();

for (let i = 0; i < docCount; i++) {
    const ttl = Math.floor(Math.random() * (maxTTL - minTTL + 1)) + minTTL;
    const expireAt = new Date(now + ttl * 1000);

    bulk.insert({
        _id: i,
        value: "ttl-random-test",
        ttlSeconds: ttl,
        expireAt: expireAt
    });
}

bulk.execute();

print("Documents inserted.");

// -----------------------------
// Wait for expiration
// -----------------------------

print("Waiting for TTL deletions...");
let remaining = docCount;
let elapsed = 0;

while (remaining > 0) {
    sleep(1000);
    elapsed++;

    remaining = dbTest[collName].countDocuments({});
    print(`Elapsed ${elapsed}s, remaining documents: ${remaining}`);

    // Safety timeout: 120 sec
    if (elapsed > 120) {
        print("Timeout reached. Some documents may not have expired.");
        break;
    }
}

assert.eq(0, remaining, "All documents expired and deleted.")

// -----------------------------
// Restore original TTL parameter
// -----------------------------

print(`Restoring ttlMonitorSleepSecs to ${originalTTL}...`);
setParameterValue("ttlMonitorSleepSecs", originalTTL);

print("Test completed.");
