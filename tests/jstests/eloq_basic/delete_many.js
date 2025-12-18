// Drop old collection
db.sk_test.drop();

// Create new collection
db.createCollection("sk_test");

// Create index on sk
db.sk_test.createIndex({ sk: 1 });

// Insert 10,000 documents
let bulk = db.sk_test.initializeUnorderedBulkOp();
for (let i = 0; i < 10000; i++) {
    bulk.insert({
        _id: i,
        sk: i,
        value: "test" + i
    });
}
bulk.execute();

// Delete all documents using sk index
print("Start delete by sk...");
let start = new Date();
db.sk_test.deleteMany(
    { sk: { $gte: 0 } },
    { hint: { sk: 1 } }
);
let end = new Date();
print("Delete by sk time(ms): " + (end - start));
print("Remaining docs after sk delete: " + db.sk_test.countDocuments({}));

// Re-insert documents
bulk = db.sk_test.initializeUnorderedBulkOp();
for (let i = 0; i < 10000; i++) {
    bulk.insert({
        _id: i,
        sk: i,
        value: "test" + i
    });
}
bulk.execute();

// Delete all documents using _id index
print("Start delete by _id...");
start = new Date();
db.sk_test.deleteMany(
    { _id: { $gte: 0 } },
    { hint: { _id: 1 } }
);
end = new Date();
print("Delete by _id time(ms): " + (end - start));
print("Remaining docs after _id delete: " + db.sk_test.countDocuments({}));
