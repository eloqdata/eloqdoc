/**
 * Point lookups on _id for values whose type the index key bytes do not record. A KeyString
 * encodes numbers by value, so the index cursor must decode the key it found with the type bits
 * stored in the entry. A Debug server checks in findSingle that the decoded key equals the search
 * key, so a cursor that dropped the type bits aborted it on every find, update or remove by one of
 * these _ids: an int64 or a double beyond int32 was narrowed, and a fractional double or decimal
 * failed to decode (error 50810). A Release server never reads the key back.
 */
(function() {
    "use strict";

    const testDB = db.getSiblingDB("eloq_basic_id_point_lookup_types");
    assert.commandWorked(testDB.dropDatabase());
    assert.commandWorked(testDB.createCollection("c"));
    const coll = testDB.c;

    // "s" follows an entry with type bits, so its lookup reuses the cursor's record after one
    // that carried unpack info.
    const ids = [
        NumberLong("5000000000"),
        "s",
        NumberLong("-5000000000"),
        NumberLong(7),
        6e9,
        1.5,
        -2.25,
        NumberDecimal("2.5"),
        NumberInt(5),
    ];
    ids.forEach((id, i) => assert.commandWorked(coll.insert({_id: id, v: i})));

    ids.forEach((id, i) => {
        const doc = coll.findOne({_id: id});
        assert.neq(null, doc, "find by " + tojson(id));
        assert.eq(tojson(doc._id), tojson(id));
        assert.eq(doc.v, i, tojson(doc));

        const res = assert.commandWorked(coll.update({_id: id}, {$set: {u: i}}));
        assert.eq(1, res.nMatched, "update by " + tojson(id));
    });

    // A numerically equal value of another type finds the same entry, keeping its stored type.
    assert.eq(tojson(coll.findOne({_id: 5e9})._id), tojson(NumberLong("5000000000")));
    assert.eq(tojson(coll.findOne({_id: NumberLong(5)})._id), tojson(NumberInt(5)));
    assert.eq(tojson(coll.findOne({_id: NumberDecimal("1.5")})._id), tojson(1.5));

    ids.forEach((id) => {
        assert.eq(1, assert.commandWorked(coll.remove({_id: id})).nRemoved, "remove " + tojson(id));
        assert.eq(null, coll.findOne({_id: id}), tojson(id));
    });
    assert.eq(0, coll.find().itcount());
})();
