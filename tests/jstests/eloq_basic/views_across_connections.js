/**
 * A view is data: every lookup reads its system.views document and reuses a cached definition only
 * if it was built from the version read. EloqDoc keeps a view catalog per worker thread, and a view
 * change used to refresh only the catalog of the thread that made it, so connections served by
 * other threads listed and queried older views.
 *
 * Here several connections, which the server spreads over its worker threads, each see at once
 * every view change made on another: a create, a collMod, a view on a view and a redefinition of
 * the view under it, a drop, a collection created under the dropped view's name, and a
 * dropDatabase.
 */
(function() {
    "use strict";

    const dbName = "eloq_views_across_connections";
    const conns = [];
    for (let i = 0; i < 8; i++) {
        conns.push(new Mongo(db.getMongo().host));
    }
    const dbs = conns.map(conn => conn.getDB(dbName));
    assert.commandWorked(dbs[0].dropDatabase());
    assert.commandWorked(dbs[0].base.insert([{_id: 1, a: 1}, {_id: 2, a: 2}, {_id: 3, a: 3}]));

    /** The views a connection lists, by name, with their pipelines. */
    function viewsOf(testDB) {
        const views = {};
        testDB.getCollectionInfos({type: "view"}).forEach(info => {
            views[info.name] = info.options.pipeline;
        });
        return views;
    }

    /**
     * Checks that every connection lists exactly 'expected' views and reads 'counts' documents
     * through the named namespaces.
     */
    function check(expected, counts, what) {
        dbs.forEach((testDB, i) => {
            assert.eq(viewsOf(testDB), expected, what + ": views on connection " + i);
            for (let name of Object.keys(counts)) {
                assert.eq(testDB[name].find().itcount(),
                          counts[name],
                          what + ": " + name + " on connection " + i);
            }
        });
    }

    const atLeast2 = [{$match: {a: {$gte: 2}}}];
    assert.commandWorked(dbs[1].createView("v", "base", atLeast2));
    check({v: atLeast2}, {v: 2}, "after the create");

    const only3 = [{$match: {a: 3}}];
    assert.commandWorked(dbs[2].runCommand({collMod: "v", viewOn: "base", pipeline: only3}));
    check({v: only3}, {v: 1}, "after the collMod");

    const project = [{$project: {a: 1}}];
    assert.commandWorked(dbs[3].createView("vv", "v", project));
    check({v: only3, vv: project}, {v: 1, vv: 1}, "after a view on the view");

    // Redefining the inner view changes what the outer one reads.
    const atMost2 = [{$match: {a: {$lte: 2}}}];
    assert.commandWorked(dbs[4].runCommand({collMod: "v", viewOn: "base", pipeline: atMost2}));
    check({v: atMost2, vv: project}, {v: 2, vv: 2}, "after redefining the inner view");

    // The outer view now reads a namespace that does not exist.
    assert(dbs[5].v.drop());
    check({vv: project}, {vv: 0}, "after the drop");

    // A collection under the dropped view's name, which the outer view reads.
    assert.commandWorked(dbs[6].createCollection("v"));
    assert.commandWorked(dbs[6].v.insert({_id: "doc", a: 7}));
    check({vv: project}, {v: 1, vv: 1}, "after a collection took the name");
    dbs.forEach((testDB, i) => {
        assert.eq(testDB.getCollectionInfos({name: "v"})[0].type,
                  "collection",
                  "v on connection " + i);
        assert.commandFailedWithCode(testDB.createView("v", "base", []),
                                     ErrorCodes.NamespaceExists,
                                     "a view over the collection v on connection " + i);
    });

    assert.commandWorked(dbs[7].dropDatabase());
    check({}, {}, "after the dropDatabase");
})();
