/**
 * Verifies that concurrent createIndexes commands rebuild their catalog state after a
 * WriteConflict instead of reusing a stale Collection clone or staged metadata.
 *
 * The workers use independent Mongo connections and issue raw createIndexes commands, so no
 * client-side retry hides a server-side failure. The pre-existing collection case must observe a
 * server WriteConflict and still return success for every command.
 */
(function() {
    'use strict';

    load('tests/jstests/libs/parallelTester.js');  // For ScopedThread and CountDownLatch.

    const dbName = 'create_indexes_catalog_write_conflict_retry';
    const collName = 'agile_work_items';
    const autoCreateCollName = 'auto_create_agile_work_items';
    const documentCount = 5000;
    const maxTimeMS = 55 * 1000;
    const testDB = db.getSiblingDB(dbName);

    const parallelSpecs = [
        {key: {team: 1, parent_id: 1, project_id: 1}, name: 'team_1_parent_id_1_project_id_1'},
        {
            key: {team: 1, 'properties.iteration': 1, project_id: 1},
            name: 'team_1_properties_iteration_1_project_id_1'
        },
        {key: {team: 1, assignee: 1}, name: 'team_1_assignee_1'},
        {key: {team: 1, short_id: 1}, name: 'team_1_short_id_1'},
        {
            key: {team: 1, 'properties.board_id': 1, project_id: 1},
            name: 'team_1_properties_board_id_1_project_id_1'
        }
    ];

    function getWriteConflictCount(database) {
        const status = database.getSiblingDB('admin').runCommand({serverStatus: 1});
        assert.commandWorked(status);
        assert.neq(undefined,
                   status.metrics.operation.writeConflicts,
                   'serverStatus must expose the write-conflict counter');
        return status.metrics.operation.writeConflicts;
    }

    function assertExpectedIndexes(coll, specs) {
        const indexes = coll.getIndexes();
        const byName = {};
        indexes.forEach(function(index) {
            byName[index.name] = index;
        });

        assert.eq(specs.length + 1, indexes.length, tojson(indexes));
        assert(byName.hasOwnProperty('_id_'), 'missing _id_ index: ' + tojson(indexes));
        specs.forEach(function(spec) {
            assert(byName.hasOwnProperty(spec.name),
                   'missing index ' + spec.name + ': ' + tojson(indexes));
            assert.docEq(spec.key, byName[spec.name].key, tojson(byName[spec.name]));
        });
    }

    function createIndexWorker(
        testData, host, databaseName, collectionName, spec, timeoutMillis, barrier) {
        // Preserve auth-passthrough state when this test is included in such a suite.
        TestData = testData;

        const connection = new Mongo(host);
        const workerDB = connection.getDB(databaseName);
        barrier.countDown();
        while (barrier.getCount() > 0) {
            // Wait until every worker has established its own connection before issuing the DDL.
        }

        return workerDB.runCommand({
            createIndexes: collectionName,
            indexes: [spec],
            maxTimeMS: timeoutMillis
        });
    }

    function runConcurrentCreateIndexes(database, collectionName, specs) {
        const barrier = new CountDownLatch(specs.length + 1);
        const threads = [];
        const conflictsBefore = getWriteConflictCount(database);

        specs.forEach(function(spec) {
            const thread = new ScopedThread(createIndexWorker,
                                            TestData,
                                            database.getMongo().host,
                                            database.getName(),
                                            collectionName,
                                            spec,
                                            maxTimeMS,
                                            barrier);
            threads.push(thread);
            thread.start();
        });

        assert.soon(function() {
            return barrier.getCount() === 1;
        }, 'not every createIndexes worker reached the start barrier', 30 * 1000);
        barrier.countDown();

        threads.forEach(function(thread) {
            thread.join();
            assert.commandWorked(thread.returnData());
        });

        return {before: conflictsBefore, after: getWriteConflictCount(database)};
    }

    function makeDocuments() {
        const documents = [];
        for (let id = 0; id < documentCount; ++id) {
            documents.push({
                team: id % 8,
                parent_id: id,
                project_id: id % 13,
                assignee: 'user-' + (id % 17),
                short_id: id,
                properties: {iteration: id % 5, board_id: id % 3}
            });
        }
        return documents;
    }

    try {
        assert.commandWorked(testDB.dropDatabase());

        // This is the regression path: all commands see the same existing catalog, then race to
        // add distinct indexes. A successful retry must use the winner's latest index metadata.
        assert.commandWorked(testDB.runCommand({
            insert: collName,
            documents: makeDocuments(),
            ordered: true
        }));
        const existingCollectionConflicts =
            runConcurrentCreateIndexes(testDB, collName, parallelSpecs);
        assert.gt(existingCollectionConflicts.after,
                  existingCollectionConflicts.before,
                  'expected a server-side WriteConflict during concurrent createIndexes: ' +
                      tojson(existingCollectionConflicts));

        const existingCollection = testDB.getCollection(collName);
        assertExpectedIndexes(existingCollection, parallelSpecs);
        assert.eq(documentCount, existingCollection.count());
        assert.commandWorked(testDB.runCommand({ping: 1}));

        // A separate empty namespace covers concurrent automatic collection creation. The server
        // may serialize this short path, so success and final catalog state are the assertions.
        const autoCreateConflicts =
            runConcurrentCreateIndexes(testDB, autoCreateCollName, parallelSpecs);
        const autoCreatedCollection = testDB.getCollection(autoCreateCollName);
        assertExpectedIndexes(autoCreatedCollection, parallelSpecs);
        assert.commandWorked(testDB.runCommand({ping: 1}));

        print('CREATE_INDEXES_CATALOG_WRITE_CONFLICT_RETRY_OK ' +
              tojsononeline({
                  existingCollectionConflicts: existingCollectionConflicts,
                  autoCreateConflicts: autoCreateConflicts,
                  indexes: parallelSpecs.map(function(spec) {
                      return spec.name;
                  })
              }));
    } finally {
        testDB.dropDatabase();
    }
}());
