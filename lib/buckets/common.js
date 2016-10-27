/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2016, Joyent, Inc.
 */

var util = require('util');

var assert = require('assert-plus');
var once = require('once');
var vasync = require('vasync');

var mod_errors = require('../errors');
var InvalidBucketConfigError = mod_errors.InvalidBucketConfigError;
var InvalidBucketNameError = mod_errors.InvalidBucketNameError;
var NotFunctionError = mod_errors.NotFunctionError;

var typeToPg = require('../pg').typeToPg;



///--- Globals

var INDEX_TYPES = {
    string: true,
    number: true,
    'boolean': true,
    ip: true,
    subnet: true
};

// Postgres rules:
// start with a letter, everything else is alphum or '_', and must be
// <= 63 characters in length
var BUCKET_NAME_RE = /^[a-zA-Z]\w{0,62}$/;

var RESERVED_BUCKETS = ['moray', 'search'];


///--- API

function buildIndexString(schema) {
    assert.object(schema, 'schema');

    var str = '';
    Object.keys(schema).forEach(function (k) {
        str += ',\n        ' + k + ' ' + typeToPg(schema[k].type);
        if (schema[k].unique)
            str += ' UNIQUE';
    });

    return (str);
}


function mapIndexType(schema, name) {
    var i = {
        name: name
    };

    if (schema && schema[name] && schema[name].type &&
        /^\[\w+\]$/.test(schema[name].type)) {
        i.type = 'GIN';
    } else {
        i.type = 'BTREE';
    }

    return (i);
}


function createIndexes(opts, cb) {
    var bucket = opts.bucket;
    var log = opts.log;
    var pg = opts.pg;

    var queries = opts.indexes.map(function (i) {
        var sql = util.format(('CREATE %s INDEX %s_%s_idx ' +
                               'ON %s USING %s (%s) ' +
                               'WHERE %s IS NOT NULL'),
                              (opts.unique ? 'UNIQUE' : ''), bucket, i.name,
                              bucket, i.type, i.name,
                              i.name);
        return (sql);
    });

    cb = once(cb);

    log.debug({bucket: bucket}, 'createIndexes: entered');
    vasync.forEachParallel({
        func: function createIndex(sql, _cb) {
            _cb = once(_cb);
            log.debug('createIndexes: running %s', sql);
            var q = pg.query(sql);
            q.once('error', function (err) {
                if (err) {
                    log.error({
                        err: err,
                        sql: sql
                    }, 'createIndex: failed');
                }
                _cb(err);
            });
            q.once('end', function () {
                _cb();
            });
        },
        inputs: queries
    }, cb);
}


function validateIndexes(schema) {
    var i, j, k, k2, keys, msg, sub, subKeys;

    keys = Object.keys(schema);
    for (i = 0; i < keys.length; i++) {
        k = keys[i];
        if (typeof (k) !== 'string') {
            throw new InvalidBucketConfigError('keys must be ' +
                                               'strings');
        }
        if (typeof (schema[k]) !== 'object') {
            throw new InvalidBucketConfigError('values must be ' +
                                               'objects');
        }

        sub = schema[k];
        subKeys = Object.keys(sub);
        for (j = 0; j < subKeys.length; j++) {
            k2 = subKeys[j];
            switch (k2) {
            case 'type':
                if (sub[k2] !== 'string' &&
                    sub[k2] !== 'number' &&
                    sub[k2] !== 'boolean' &&
                    sub[k2] !== 'ip' &&
                    sub[k2] !== 'subnet' &&
                    sub[k2] !== '[string]' &&
                    sub[k2] !== '[number]' &&
                    sub[k2] !== '[boolean]' &&
                    sub[k2] !== '[ip]' &&
                    sub[k2] !== '[subnet]') {
                    msg = k + '.type is invalid';
                    throw new InvalidBucketConfigError(msg);
                }
                break;
            case 'unique':
                if (typeof (sub[k2]) !== 'boolean') {
                    msg = k + '.unique must be boolean';
                    throw new InvalidBucketConfigError(msg);
                }
                break;
            default:
                msg = k + '.' + k2 + ' is invalid';
                throw new InvalidBucketConfigError(msg);
            }
        }
    }
}


function validateBucket(req, cb) {
    var bucket = req.bucket;
    var log = req.log;

    log.debug('validate: entered (%j)', bucket);

    bucket.index = bucket.index || {};
    bucket.post = bucket.post || [];
    bucket.pre = bucket.pre || [];

    try {
        bucket.post = bucket.post.map(function (p) {
            var fn;
            fn = eval('fn = ' + p);
            return (fn);
        });
        bucket.pre = bucket.pre.map(function (p) {
            var fn;
            fn = eval('fn = ' + p);
            return (fn);
        });
    } catch (e) {
        log.debug(e, 'Invalid trigger function(s)');
        return (cb(new NotFunctionError(e, 'trigger not function')));
    }

    if (!BUCKET_NAME_RE.test(bucket.name))
        return (cb(new InvalidBucketNameError(bucket.name)));

    if (RESERVED_BUCKETS.indexOf(bucket.name) !== -1)
        return (cb(new InvalidBucketNameError(bucket.name)));

    if (typeof (bucket.index) !== 'object' ||
        Array.isArray(bucket.index)) {
        return (cb(new InvalidBucketConfigError('index is not ' +
                                                'an object')));
    }

    if (!Array.isArray(bucket.post))
        return (cb(new NotFunctionError('post')));

    try {
        assert.arrayOfFunc(bucket.post);
    } catch (e) {
        log.debug(e, 'validation of post failed');
        return (cb(new NotFunctionError('post')));
    }

    if (!Array.isArray(bucket.pre))
        return (cb(new NotFunctionError('pre')));

    try {
        assert.arrayOfFunc(bucket.pre);
    } catch (e) {
        log.debug(e, 'validation of pre failed');
        return (cb(new NotFunctionError('pre')));
    }

    try {
        validateIndexes(bucket.index);
    } catch (e) {
        return (cb(e));
    }

    if (typeof (bucket.options) !== 'object') {
        return (cb(new InvalidBucketConfigError('options is not ' +
                                                'an object')));
    }

    if (typeof (bucket.options.version) !== 'number') {
        return (cb(new InvalidBucketConfigError('options.version ' +
                                                'is not a number')));
    }

    log.debug('validate: done');
    return (cb());
}

function ensureRowVer(req, cb) {
    // If a reindex operation has been requested, updated/pending rows will
    // need a _rver column to track completion.  Since this will be in use as
    // soon as the bucket record is updated with the reindexing operation, it
    // _must_ exist beforehand.

    if (req.opts.no_reindex || !req.bucket.options.version) {
        // skip if bucket is versionless or reindexing excluded
        cb(null);
        return;
    }

    cb = once(cb);
    var b = req.bucket;
    var log = req.log;
    var pg = req.pg;
    var sql, q;

    log.debug({
        bucket: b.name
    }, 'ensureRowVer: entered');

    vasync.pipeline({
        funcs: [
            function checkCol(arg, callback) {
                sql = 'SELECT column_name FROM information_schema.columns ' +
                    'WHERE table_name = $1 AND column_name = $2';
                q = pg.query(sql, [b.name, arg.colName]);
                q.on('error', callback);
                q.once('row', function () {
                    arg.colExists = true;
                });
                q.once('end', function () {
                    callback(null);
                });
            },
            function addCol(arg, callback) {
                if (arg.colExists) {
                    callback(null);
                    return;
                }
                sql = util.format('ALTER TABLE %s ADD COLUMN ', b.name) +
                    arg.colName + ' INTEGER';
                q = pg.query(sql);
                q.on('error', callback);
                q.once('end', callback.bind(null, null));
            },
            function checkIdx(arg, callback) {
                sql = 'SELECT indexname FROM pg_catalog.pg_indexes ' +
                    'WHERE tablename = $1 AND indexname = $2';
                q = pg.query(sql, [b.name, arg.idxName]);
                q.on('error', callback);
                q.once('row', function () {
                    arg.idxExists = true;
                });
                q.once('end', function () {
                    callback(null);
                });
            },
            function addIdx(arg, callback) {
                if (arg.idxExists) {
                    callback(null);
                    return;
                }
                createIndexes({
                    bucket: b.name,
                    log: log,
                    pg: pg,
                    indexes: [
                        {name: arg.colName, type: 'BTREE'}
                    ]
                }, callback);
            }
        ],
        arg: {
            colName: '_rver',
            idxName: b.name + '__rver_idx',
            colExists: false,
            idxExists: false
        }
    }, function (err, res) {
        log.debug({
            bucket: b.name,
            err: err
        }, 'ensureRowVer: failed');
        cb(err);
    });
}


///--- Exports

module.exports = {
    INDEX_TYPES: INDEX_TYPES,
    BUCKET_NAME_RE: BUCKET_NAME_RE,
    RESERVED_BUCKETS: RESERVED_BUCKETS,
    buildIndexString: buildIndexString,
    createIndexes: createIndexes,
    mapIndexType: mapIndexType,
    validateBucket: validateBucket,
    ensureRowVer: ensureRowVer
};
