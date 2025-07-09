(function(){
    'use strict'

    var col = db.multikey_index;
    col.drop();
    col.createIndex({a: 1});
    col.createIndex({b: 1});
    col.insert({a: [1, 2, 3], b: ['a', 'b', 'c']});
    col.insert({a: [4, 5, 6], b: ['d', 'e', 'f']});
    assert.eq(2, col.count({a: {$gte: 3}}), "A");
    assert.eq(2, col.count({b: {$gte: 'c'}}), "B");

    col.drop();
    col.createIndex({a: 1});
    col.createIndex({b: 1});
    col.insert({a: [1, 2, 3]});
    col.insert({a: [4, 5, 6]});
    col.insert({b: ['a', 'b', 'c']});
    col.insert({b: ['d', 'e', 'f']});
    assert.eq(2, col.count({a: {$gte: 3}}), "C");
    assert.eq(2, col.count({b: {$gte: 'c'}}), "D");

    col.drop();
    col.insert({a: [1, 2, 3], b: ['a', 'b', 'c']});
    col.insert({a: [4, 5, 6], b: ['d', 'e', 'f']});
    col.createIndex({a: 1});
    col.createIndex({b: 1});
    assert.eq(2, col.count({a: {$gte: 3}}), "A");
    assert.eq(2, col.count({b: {$gte: 'c'}}), "B");
})();
