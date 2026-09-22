/**
 * The watch() helpers on DBCollection, DB and Mongo, exercised with their aggregate call stubbed
 * out, so no server takes part. Each helper turns watch()'s options into a $changeStream stage and
 * passes the rest down; these cases pin that startAfter goes into the stage (docs/cdc-change-
 * stream.md §5.1, §5.6) and not down as an aggregate option, where the server would reject it as
 * unknown or ignore it and silently open a fresh stream instead of resuming.
 *
 * The three helpers reach aggregate differently -- this.aggregate, this._runAggregate, and
 * getDB("admin")._runAggregate -- so each gets its own stub, normalised to report the pipeline
 * and the options it was handed.
 */
(function() {
    "use strict";

    const kToken = {_data: "02000000000000006401000100000000"};
    const kOtherToken = {_data: "02000000000000006400000000"};

    const helpers = [
        {
            name: "DBCollection.watch",
            make: function() {
                const coll = Object.create(DBCollection.prototype);
                coll.aggregate = function(pipeline, options) {
                    return {pipeline: pipeline, options: options};
                };
                return coll;
            },
        },
        {
            name: "DB.watch",
            make: function() {
                const db = Object.create(DB.prototype);
                db._runAggregate = function(cmd, options) {
                    return {pipeline: cmd.pipeline, options: options, cmd: cmd};
                };
                return db;
            },
        },
        {
            name: "Mongo.watch",
            make: function() {
                const mongo = Object.create(Mongo.prototype);
                mongo.getDB = function(dbName) {
                    return {
                        _runAggregate: function(cmd, options) {
                            return {pipeline: cmd.pipeline, options: options, cmd: cmd, db: dbName};
                        },
                    };
                };
                return mongo;
            },
        },
    ];

    helpers.forEach(function(helper) {
        const name = helper.name;

        // startAfter lands in the $changeStream stage and is removed from the options passed down,
        // while the options that do belong to aggregate still reach it.
        (function startAfterIsForwardedIntoTheStage() {
            const res = helper.make().watch([], {startAfter: kToken, batchSize: 5});
            assert.eq(res.pipeline[0].$changeStream.startAfter, kToken, name);
            assert(!res.options.hasOwnProperty("startAfter"),
                   name + ": startAfter leaked into the aggregate options: " + tojson(res.options));
            assert.eq(res.options.batchSize, 5, name + ": batchSize did not reach aggregate");
        })();

        // The user's own stages follow the $changeStream stage, untouched.
        (function userStagesFollowTheChangeStreamStage() {
            const userStage = {$match: {operationType: "insert"}};
            const res = helper.make().watch([userStage], {startAfter: kToken});
            assert.eq(res.pipeline.length, 2, name);
            assert(res.pipeline[0].hasOwnProperty("$changeStream"), name);
            assert.eq(res.pipeline[1], userStage, name);
        })();

        // resumeAfter still works alongside the new branch.
        (function resumeAfterIsStillForwarded() {
            const res = helper.make().watch([], {resumeAfter: kToken});
            assert.eq(res.pipeline[0].$changeStream.resumeAfter, kToken, name);
            assert(!res.pipeline[0].$changeStream.hasOwnProperty("startAfter"), name);
            assert(!res.options.hasOwnProperty("resumeAfter"), name);
        })();

        // Given both, the helper forwards both rather than choosing: the server owns the rule
        // that at most one resume option may be set, and must see the conflict to report it.
        (function bothResumeOptionsAreForwardedForTheServerToReject() {
            const res = helper.make().watch([], {resumeAfter: kToken, startAfter: kOtherToken});
            const stage = res.pipeline[0].$changeStream;
            assert.eq(stage.resumeAfter, kToken, name);
            assert.eq(stage.startAfter, kOtherToken, name);
        })();

        // Without startAfter nothing is added for it.
        (function noStartAfterMeansNoField() {
            const res = helper.make().watch([], {});
            assert(!res.pipeline[0].$changeStream.hasOwnProperty("startAfter"), name);
        })();
    });

    // Mongo.watch is the deployment-level stream: it asks for every collection and runs against
    // admin, which is what the reference consumer opens (§5.1, §6.1).
    (function mongoWatchIsDeploymentLevel() {
        const res = helpers[2].make().watch([], {startAfter: kToken});
        assert.eq(res.db, "admin");
        assert.eq(res.pipeline[0].$changeStream.allChangesForCluster, true);
        assert.eq(res.pipeline[0].$changeStream.startAfter, kToken);
    })();
})();
