/**
 * A create refused for its options leaves no table behind. EloqDoc's create-table transaction
 * commits before the collection object is built, so the server must reject, before that
 * transaction, every option the collection's constructor would reject: a validator in the local,
 * admin or config database, a validator on a system collection, and an unknown validationLevel or
 * validationAction. Each such create fails, the collection does not exist afterwards, and
 * listCollections keeps working in its database; a leftover table would make listCollections fail
 * there, and the next startup too.
 */
(function() {
    "use strict";

    const validator = {a: {$exists: true}};
    const name = "eloq_basic_create_refused_options";

    /**
     * Asserts that creating dbName.collName with the options fails with the code and leaves
     * nothing behind.
     */
    function assertRefused(dbName, collName, options, code) {
        const target = db.getSiblingDB(dbName);
        const res = target.runCommand(Object.assign({create: collName}, options));
        assert.commandFailedWithCode(res, code, tojson(options));
        const listed =
            assert.commandWorked(target.runCommand({listCollections: 1, filter: {name: collName}}));
        assert.eq([], listed.cursor.firstBatch, tojson(listed));
    }

    ["local", "admin", "config"].forEach(function(internalDb) {
        assertRefused(internalDb, name, {validator: validator}, ErrorCodes.InvalidOptions);
    });

    const testDB = db.getSiblingDB(name);
    assert.commandWorked(testDB.dropDatabase());
    assertRefused(name, "system.js", {validator: validator}, ErrorCodes.InvalidOptions);
    assertRefused(name, "level", {validationLevel: "bogus"}, ErrorCodes.BadValue);
    assertRefused(name, "action", {validator: validator, validationAction: "bogus"},
                  ErrorCodes.BadValue);

    // Valid options still create the collection.
    assert.commandWorked(testDB.createCollection(
        "valid", {validator: validator, validationLevel: "moderate", validationAction: "warn"}));
    const info = testDB.getCollectionInfos({name: "valid"});
    assert.eq(1, info.length, tojson(info));
    assert.eq("moderate", info[0].options.validationLevel, tojson(info));
    assert.eq("warn", info[0].options.validationAction, tojson(info));

    assert.commandWorked(testDB.dropDatabase());
})();
