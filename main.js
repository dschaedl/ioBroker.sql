/* jshint -W097 */// jshint strict:false
/*jslint node: true */
"use strict";

var utils    = require(__dirname + '/lib/utils'); // Get common adapter utils
var SQL      = require('sql-client');
//var mySql    = require('mysql');
var ping     = require('net-ping');

var commons  = require(__dirname + '/lib/aggregate');
var SQLFuncs = null;
var fs       = require('fs');
var store    = require('node-persist');
var batch    = require('batchflow');



var clients = {
    postgresql: {name: 'PostgreSQLClient'},
    mysql:      {name: 'MySQLClient'},
    sqlite:     {name: 'SQLite3Client'},
    mssql:      {name: 'MSSQLClient'}
};

var types   = {
    'number':  0,
    'string':  1,
    'boolean': 2
};

var dbNames = [
    'ts_number',
    'ts_string',
    'ts_bool'
];

var sqlDPs  = {};
var from    = {};
var clientPool;
var subscribeAll = false;
var dbAvailable = false;
var runningprocess = false;

var adapter = utils.adapter('sql');

adapter.on('objectChange', function (id, obj) {
    if (obj && obj.common && obj.common.history && obj.common.history[adapter.namespace]) {
        if (!sqlDPs[id] && !subscribeAll) {
            // unsubscribe
            for (var id in sqlDPs) {
                adapter.unsubscribeForeignStates(id);
            }
            subscribeAll = true;
            adapter.subscribeForeignStates('*');
        }
        sqlDPs[id] = obj.common.history;
        adapter.log.info('enabled logging of ' + id);
    } else {
        if (sqlDPs[id]) {
            adapter.log.info('disabled logging of ' + id);
            delete sqlDPs[id];
        }
    }
});

adapter.on('stateChange', function (id, state) {
    pushHistory(id, state);
});

adapter.on('unload', function (callback) {
    finish(callback);
});

adapter.on('ready', function () {
    main();
});

adapter.on('message', function (msg) {
    processMessage(msg);
});

process.on('SIGINT', function () {
    // close connection to DB
    finish();
});

var _client = false;
function connect() {
    if (!clientPool) {
        var params = {
            server:     adapter.config.host + (adapter.config.port ? ':' + adapter.config.port : ''),
            host:       adapter.config.host + (adapter.config.port ? ':' + adapter.config.port : ''),
            user:       adapter.config.user,
            password:   adapter.config.password,
            max_idle:   (adapter.config.dbtype === 'sqlite') ? 1 : 2
        };
        if (adapter.config.encrypt) {
            params.options = {
                encrypt: true // Use this if you're on Windows Azure
            };
        }

        if (adapter.config.dbtype === 'postgres') {
            params.database = 'postgres';
        }

        if (adapter.config.dbtype === 'sqlite') {
            params = getSqlLiteDir(adapter.config.fileName);
        }
        else
        // special solution for postgres. Connect first to Db "postgres", create new DB "iobroker" and then connect to "iobroker" DB.
        if (_client !== true && adapter.config.dbtype === 'postgresql') {
            if (adapter.config.dbtype == 'postgresql') {
                params.database = 'postgres';
            }
            // connect first to DB postgres and create iobroker DB
            _client = new SQL[clients[adapter.config.dbtype].name](params);
            return _client.connect(function (err) {
                if (err) {
                    adapter.log.error(err);
                    setTimeout(function () {
                        connect();
                    }, 30000);
                    return;
                }
                _client.execute('CREATE DATABASE iobroker;', function (err, rows, fields) {
                    _client.disconnect();
                    if (err && err.code !== '42P04') { // if error not about yet exists
                        _client = false;
                        adapter.log.error(err);
                        setTimeout(function () {
                            connect();
                        }, 30000);
                    } else {
                        _client = true;
                        setTimeout(function () {
                            connect();
                        }, 100);
                    }
                });
            });
        }

        if (adapter.config.dbtype == "postgresql") {
            params.database = "iobroker";
        }

        try {
            clientPool = new SQL[clients[adapter.config.dbtype].name + 'Pool'](params);
            return clientPool.open(function (err) {
                if (err) {
                    adapter.log.error(err);
                    setTimeout(function () {
                        connect();
                    }, 30000);
                } else {
                    adapter.log.debug("Pool opened for new connections");
                    setTimeout(function () {
                        connect();
                    }, 0);
                }
            });
        } catch (ex) {
            if (ex.toString() == 'TypeError: undefined is not a function') {
                adapter.log.error('Node.js DB driver for "' + adapter.config.dbtype + '" could not be installed.');
            } else {
                adapter.log.error(ex.toString());
            }
            return setTimeout(function () {
                connect();
            }, 30000);
        }
    }

    allScripts(SQLFuncs.init(), function (err) {
        if (err) {
            adapter.log.error(err);
            return setTimeout(function () {
                connect();
            }, 30000);
        } else {
            adapter.log.info('Connected to ' + adapter.config.dbtype);
        }
    });
}

// Find sqlite data directory
function getSqlLiteDir(fileName) {
    fileName = fileName || 'sqlite.db';
    fileName = fileName.replace(/\\/g, '/');
    if (fileName[0] == '/' || fileName.match(/^\w:\//)) {
        return fileName;
    }
    else {
        // normally /opt/iobroker/node_modules/iobroker.js-controller
        // but can be /example/ioBroker.js-controller
        var tools = require(utils.controllerDir + '/lib/tools');
        var config = tools.getConfigFileName().replace(/\\/g, '/');
        var parts = config.split('/');
        parts.pop();
        config = parts.join('/') + '/sqlite';
        // create sqlite directory
        if (!fs.existsSync(config)) {
            fs.mkdirSync(config);
        }

        return config + '/' + fileName;
    }
}

function testConnection(msg) {
    msg.message.config.port = parseInt(msg.message.config.port, 10) || 0;
    var params = {
        server:     msg.message.config.host + (msg.message.config.port ? ':' + msg.message.config.port : ''),
        host:       msg.message.config.host + (msg.message.config.port ? ':' + msg.message.config.port : ''),
        user:       msg.message.config.user,
        password:   msg.message.config.password
    };

    if (msg.message.config.dbtype === 'postgresql' && !SQL.PostgreSQLClient) {
        var postgres = require(__dirname + '/lib/postgresql-client');
        for (var attr in postgres) {
            if (!SQL[attr]) SQL[attr] = postgres[attr];
        }
    } else
    if (msg.message.config.dbtype === 'mssql' && !SQL.MSSQLClient) {
        var mssql = require(__dirname + '/lib/mssql-client');
        for (var attr in mssql) {
            if (!SQL[attr]) SQL[attr] = mssql[attr];
        }
    }

    if (msg.message.config.dbtype === 'postgresql') {
        params.database = 'postgres';
    } else if (msg.message.config.dbtype === 'sqlite') {
        params = getSqlLiteDir(msg.message.config.fileName);
    }
    var timeout;
    try {
        var client = new SQL[clients[msg.message.config.dbtype].name](params);
        //var client = mySql.createConnection(params);
        timeout = setTimeout(function () {
            timeout = null;
            adapter.sendTo(msg.from, msg.command, {error: 'connect timeout'}, msg.callback);
        }, 5000);

        client.connect(function (err) {
            if (err) {
                if (timeout) {
                    clearTimeout(timeout);
                    timeout = null;
                }
                return adapter.sendTo(msg.from, msg.command, {error: err.toString()}, msg.callback);
            }
            client.execute("SELECT 2 + 3 AS x", function (err, rows, fields) {
                client.end();
                if (timeout) {
                    clearTimeout(timeout);
                    timeout = null;
                    return adapter.sendTo(msg.from, msg.command, {error: err ? err.toString() : null}, msg.callback);
                }
            });
        });
    } catch (ex) {
        if (timeout) {
            clearTimeout(timeout);
            timeout = null;
        }
        if (ex.toString() == 'TypeError: undefined is not a function') {
            return adapter.sendTo(msg.from, msg.command, {error: 'Node.js DB driver could not be installed.'}, msg.callback);
        } else {
            return adapter.sendTo(msg.from, msg.command, {error: ex.toString()}, msg.callback);
        }
    }
}

function destroyDB(msg) {
    try {
        allScripts(SQLFuncs.destroy(), function (err) {
            if (err) {
                adapter.log.error(err);
                adapter.sendTo(msg.from, msg.command, {error: err.toString()}, msg.callback);
            } else {
                adapter.sendTo(msg.from, msg.command, {error: null}, msg.callback);
                // restart adapter
                setTimeout(function () {
                    adapter.getForeignObject('system.adapter.' + adapter.namespace, function (err, obj) {
                        if (!err) {
                            adapter.setForeignObject(obj._id, obj);
                        } else {
                            adapter.log.error('Cannot read object "system.adapter.' + adapter.namespace + '": ' + err);
                            adapter.stop();
                        }
                    });
                }, 2000);
            }
        });
    } catch (ex) {
        return adapter.sendTo(msg.from, msg.command, {error: ex.toString()}, msg.callback);
    }
}

// one script
function oneScript(script, cb) {
    try {
        clientPool.borrow(function (err, client) {
            if (err || !client) {
                clientPool.close();
                clientPool = null;
                adapter.log.error(err);
                if (cb) cb(err);
                return;
            }
            adapter.log.debug(script);
            client.execute(script, function(err, rows, fields) {
                adapter.log.debug('Response: ' + JSON.stringify(err));
                if (err) {
                    // Database 'iobroker' already exists. Choose a different database name.
                    if (err.number === 1801 ||
                            //There is already an object named 'sources' in the database.
                        err.number === 2714) {
                        // do nothing
                        err = null;
                    } else
                    if (err.message && err.message.match(/^SQLITE_ERROR: table [\w_]+ already exists$/)) {
                        // do nothing
                        err = null;
                    } else
                    if (err.errno == 1007 || err.errno == 1050) { // if database exists or table exists
                        // do nothing
                        err = null;
                    }  else
                    if (err.code == '42P04') {// if database exists or table exists
                        // do nothing
                        err = null;
                    }
                    else if (err.code == '42P07') {
                        var match = script.match(/CREATE\s+TABLE\s+(\w*)\s+\(/);
                        if (match) {
                            adapter.log.debug('OK. Table "' + match[1] + '" yet exists');
                            err = null;
                        } else {
                            adapter.log.error(script);
                            adapter.log.error(err);
                        }
                    } else {
                        adapter.log.error(script);
                        adapter.log.error(err);
                    }
                }
                if (cb) cb(err);
                clientPool.return(client);
            });
        });
    } catch(ex) {
        adapter.log.error(ex);
        if (cb) cb(ex);
    }

}

// all scripts
function allScripts(scripts, index, cb) {
    if (typeof index === 'function') {
        cb = index;
        index = 0;
    }
    index = index || 0;

    if (scripts && index < scripts.length) {
        oneScript(scripts[index], function (err) {
            if (err) {
                cb && cb(err);
            } else {
                allScripts(scripts, index + 1, cb);
            }
        });
    } else {
        cb && cb();
    }
}

function finish(callback) {
    adapter.log.info("starting finish process for SQL Adapter");
    store.persistSync();
    if (clientPool) clientPool.close();
    if (callback)   callback();
}

function processMessage(msg) {
    if (msg.command == 'getHistory') {
        getHistory(msg);
    } else if (msg.command == 'test') {
        testConnection(msg);
    } else if (msg.command == 'destroy') {
        destroyDB(msg);
    } else if (msg.command == 'generateDemo') {
        generateDemo(msg)
    }
}

function main() {

    if (!clients[adapter.config.dbtype]) {
        adapter.log.error('Unknown DB type: ' + adapter.config.dbtype);
        adapter.stop();
    }
    adapter.config.port = parseInt(adapter.config.port, 10) || 0;
    if (adapter.config.round !== null && adapter.config.round !== undefined) {
        adapter.config.round = Math.pow(10, parseInt(adapter.config.round, 10));
    } else {
        adapter.config.round = null;
    }
    if (adapter.config.dbtype === 'postgresql' && !SQL.PostgreSQLClient) {
        var postgres = require(__dirname + '/lib/postgresql-client');
        for (var attr in postgres) {
            if (!SQL[attr]) SQL[attr] = postgres[attr];
        }
    } else
    if (adapter.config.dbtype === 'mssql' && !SQL.MSSQLClient) {
        var mssql = require(__dirname + '/lib/mssql-client');
        for (var attr in mssql) {
            if (!SQL[attr]) SQL[attr] = mssql[attr];
        }
    }

    SQLFuncs = require(__dirname + '/lib/' + adapter.config.dbtype);

    // read all history settings
    adapter.objects.getObjectView('history', 'state', {}, function (err, doc) {
        var count = 0;
        if (doc && doc.rows) {
            for (var i = 0, l = doc.rows.length; i < l; i++) {
                if (doc.rows[i].value) {
                    var id = doc.rows[i].id;
                    sqlDPs[id] = doc.rows[i].value;

                    if (!sqlDPs[id][adapter.namespace]) {
                        delete sqlDPs[id];
                    } else {
                        count++;
                        adapter.log.debug('enabled logging of ' + id);
                        if (sqlDPs[id][adapter.namespace].retention !== undefined && sqlDPs[id][adapter.namespace].retention !== null && sqlDPs[id][adapter.namespace].retention !== '') {
                            sqlDPs[id][adapter.namespace].retention = parseInt(sqlDPs[id][adapter.namespace].retention || adapter.config.retention, 10) || 0;
                        } else {
                            sqlDPs[id][adapter.namespace].retention = adapter.config.retention;
                        }

                        if (sqlDPs[id][adapter.namespace].debounce !== undefined && sqlDPs[id][adapter.namespace].debounce !== null && sqlDPs[id][adapter.namespace].debounce !== '') {
                            sqlDPs[id][adapter.namespace].debounce = parseInt(sqlDPs[id][adapter.namespace].debounce, 10) || 0;
                        } else {
                            sqlDPs[id][adapter.namespace].debounce = adapter.config.debounce;
                        }
                        sqlDPs[id][adapter.namespace].changesOnly = sqlDPs[id][adapter.namespace].changesOnly === 'true' || sqlDPs[id][adapter.namespace].changesOnly === true;

                        // add one day if retention is too small
                        if (sqlDPs[id][adapter.namespace].retention <= 604800) {
                            sqlDPs[id][adapter.namespace].retention += 86400;
                        }
                    }
                }
            }
        }
        if (count < 20) {
            for (var id in sqlDPs) {
                adapter.subscribeForeignStates(id);
            }
        } else {
            subscribeAll = true;
            adapter.subscribeForeignStates('*');
        }
    });

    adapter.subscribeForeignObjects('*');

    if (adapter.config.dbtype === 'sqlite' || adapter.config.host) {
        connect();
    }

    store.initSync({continuous: false, interval: 1000*60*10, ttl: 1000*60*60*24*10});
    setInterval(function() {adapter.log.info("setinterval for dumpToDB to " + settings.dbpush + "min"); dumpCacheToDB();}, 1000*60*settings.dbpush);
    setInterval(function() {adapter.log.info("setinterval for checkCacheRetention to 600s"); checkCacheRetention();}, 1000*600);
}

function pushHistory(id, state) {
    if (sqlDPs[id]) {
        var settings = sqlDPs[id][adapter.namespace];

        if (!settings || !state) return;

        if (sqlDPs[id].state && settings.changesOnly && (state.ts !== state.lc)) return;

        sqlDPs[id].state = state;

        // Do not store values ofter than 1 second
        if (!sqlDPs[id].timeout && settings.debounce) {
            sqlDPs[id].timeout = setTimeout(pushHelper, settings.debounce, id);
        } else if (!settings.debounce) {
            pushHelper(id);
        }
    }
}

function pushHelper(_id) {
    if (!sqlDPs[_id] || !sqlDPs[_id].state || !sqlDPs[_id].state.val) return;
    var _settings = sqlDPs[_id][adapter.namespace];
    // if it was not deleted in this time
    if (_settings) {
        sqlDPs[_id].timeout = null;

        if (typeof sqlDPs[_id].state.val === 'string') {
            var f = parseFloat(sqlDPs[_id].state.val);
            if (f.toString() == sqlDPs[_id].state.val) {
                sqlDPs[_id].state.val = f;
            } else if (sqlDPs[_id].state.val === 'true') {
                sqlDPs[_id].state.val = true;
            } else if (sqlDPs[_id].state.val === 'false') {
                sqlDPs[_id].state.val = false;
            }
        }

        // store to local cache - always
        adapter.log.debug("storing into local cache - id: " + _id);
        pushStateIntoLocalCache(_id, sqlDPs[_id].state);
    }
}

function pushStateIntoLocalCache(id, state) {
    var content = [state];
    adapter.log.debug('writing to local node-persist cache: ' + id + ': ' + JSON.stringify(state));
    var entry = store.getItemSync(id);
    if (!entry) {
        adapter.log.debug('this is a new entry: ' + id);
        entry = [];
    }
    entry.push(state);
    store.setItem(id, entry);
    adapter.log.debug('writing to cache was successfull - ' + content.length);
}

function dumpCacheToDB() {
    adapter.log.info("start dumping Cache to DB");

    var ids = [];

    // opening connection
    var params = {
        host:       adapter.config.host + (adapter.config.port ? ':' + adapter.config.port : ''),
    };

    //check ping
    adapter.log.debug("dumping: creating ping session");
    var session = ping.createSession();
    session.pingHost(params.host, function(error, target){
        if (error) {
            adapter.log.debug("ping to host throws an error: " + error.toString());
            dbAvailable = false;
        }
        else if (!runningprocess) {
            dbAvailable = true;
            runningprocess = true;

            var ids = store.keys();
            adapter.log.debug("got all IDs: " + ids.length);
            // iterate all IDs and write to DB
            // borrowing a client

            adapter.log.debug("borrowing a SQL client ");
            clientPool.borrow(function (err, client) {
                if (err) {
                    adapter.log.error('error connecting to DB: ' + err.stack);
                    runningprocess = false;
                    return;
                }
                adapter.log.debug("client connected to DB: " + client.threadId);

                var counter = 0;
                batch(ids).sequential().each(function(i, id, next) {
                    adapter.log.debug("iterating ID: " + id);
                    insertObjectIntoDB(id, client, next);
                    counter++;
                }).error(function(err){
                    adapter.log.error("error iterating ids: " + err.stack);
                    clientPool.return(client);
                    runningprocess = false;
                }).end(function() {
                    adapter.log.debug("iterating ended");
                    clientPool.return(client);
                    session.close();
                    runningprocess = false;
                    adapter.log.infos("dumping ende successfully");
                });
            });
        } else {
            adapter.log.warn("sql dumping process is still active...");
        }
        adapter.log.debug("dumping done for this time...");
    });
}

function insertObjectIntoDB(id, client, next) {
    var entries = store.getItemSync(id);
    adapter.log.debug("iterating states for: " + id + " - lenght: " + entries.length);

    batch(entries).sequential().each(function(i, item, next) {
//        adapter.log.debug("writing state into DB: " + JSON.stringify(item));
        insertStateIntoDB(id, item, client, next);
    }).error(function(err){
        adapter.log.error("error iterating entry: " + err.stack);
    }).end(function() {
//        adapter.log.debug("iterating ended2: ");
        next();
    });
}

function insertStateIntoDB(id, state, client, next) {
    var type = types[typeof state.val];
    if (type === undefined) {
        adapter.log.warn('Cannot store values of type "' + typeof state.val + '" - value: ' + JSON.stringify(state.val));
        next();
        return;
    }
    // get id if state
    if (!sqlDPs[id] || sqlDPs[id].index === undefined) {
        // read or create in DB
        return getId(id, type, client, function (err) {
            if (err) {
                adapter.log.warn('Cannot get index of "' + id + '": ' + err);
            } else {
                insertStateIntoDB(id, state, client, next);
            }
        });
    }

    // get from
    if (state.from && !from[state.from]) {
        // read or create in DB
        return getFrom(state.from, client, function (err) {
            if (err) {
                adapter.log.warn('Cannot get "from" for "' + state.from + '": ' + err);
            } else {
                insertStateIntoDB(id, state, client, next);
            }
        });
    }

    // if not yet stored and valid date
    if (!state.stored && state.ts > 1000000000 && state.ts < 9000000000) {
        var query = SQLFuncs.insert(sqlDPs[id].index, state, from[state.from] || 0, dbNames[type]);
//    adapter.log.debug("now query: " + query);

        client.execute(query, function (err, rows, fields) {
            if (err) {
                adapter.log.error('Cannot insert ' + query + ': ' + err);
            }
            adapter.log.debug("query executed: " + client.threadId + " - " + query);
            state.stored = true;
            next();
        });
    } else {
//            adapter.log.debug("state is already stored - do nothing");
        next();
    }
}

function getId(id, type, client, cb) {
    var query = SQLFuncs.getIdSelect(id);
    client.execute(query, function (err, rows, fields) {
        adapter.log.debug("query executed: " + client.threadId + " - " + query);
        if (rows && rows.rows) rows = rows.rows;
        if (err) {
            adapter.log.error('Cannot select1 ' + query + ': ' + err);
            if (cb) cb(err);
            return;
        }
        if (!rows.length) {
            if (type !== null) {
                // insert
                query = SQLFuncs.getIdInsert(id, type);
                client.execute(query, function (err, rows, fields) {
                    if (err) {
                        adapter.log.error('Cannot insert ' + query + ': ' + err);
                        if (cb) cb(err);
                        return;
                    }
                    adapter.log.debug("query executed: " + client.threadId + " - " + query);
                    query = SQLFuncs.getIdSelect(id);
                    client.execute(query, function (err, rows, fields) {
                        if (rows && rows.rows) rows = rows.rows;
                        if (err) {
                            adapter.log.error('Cannot select2 ' + query + ': ' + err);
                            if (cb) cb(err);
                            return;
                        }
                        adapter.log.debug("query executed: " + client.threadId + " - " + query);
                        sqlDPs[id].index = rows[0].id;
                        sqlDPs[id].type  = rows[0].type;

                        if (cb) cb();
                    });
                });
            } else {
                if (cb) cb('id not found');
            }
        } else {
            sqlDPs[id].index = rows[0].id;
            sqlDPs[id].type  = rows[0].type;

            if (cb) cb();
        }
    });
}

function getFrom(_from, client, cb) {
    var sources    = (adapter.config.dbtype !== 'postgresql' ? "iobroker." : "") + "sources";
    var query = SQLFuncs.getFromSelect(_from);

    client.execute(query, function (err, rows, fields) {
        adapter.log.debug("query executed: " + client.threadId + " - " + query);
        if (rows && rows.rows) rows = rows.rows;
        if (err) {
            adapter.log.error('Cannot select3 ' + query + ': ' + err);
            if (cb) cb(err);
            return;
        }
        if (!rows.length) {
            // insert
            query = SQLFuncs.getFromInsert(_from);
            client.execute(query, function (err, rows, fields) {
                if (err) {
                    adapter.log.error('Cannot insert ' + query + ': ' + err);
                    if (cb) cb(err);
                    return;
                }
                adapter.log.debug("query executed: " + client.threadId + " - " + query);

                query = SQLFuncs.getFromSelect(_from);
                client.execute(query, function (err, rows, fields) {
                    if (rows && rows.rows) rows = rows.rows;
                    if (err) {
                        adapter.log.error('Cannot select4 ' + query + ': ' + err);
                        if (cb) cb(err);
                        return;
                    }
                    adapter.log.debug("query executed: " + client.threadId + " - " + query);
                    from[_from] = rows[0].id;

                    if (cb) cb();
                });
            });
        } else {
            from[_from] = rows[0].id;

            if (cb) cb();
        }
    });
}

function checkCacheRetention() {
    var params = {
        host:       adapter.config.host + (adapter.config.port ? ':' + adapter.config.port : ''),
    };
    //check ping
    adapter.log.debug("checkCacheRetention: starting up");
    var session = ping.createSession();

    if (!runningprocess) {
        runningprocess = true;
        var ids = store.keys();
        adapter.log.debug("checkCacheRetention got all IDs: " + ids.length);

        // calculate last valid Date
        var retentionTime = new Date();
        retentionTime.setDate(retentionTime.getDate() - settings.cacheretention);

        batch(ids).sequential().each(function (i, id, next) {
            adapter.log.debug("checkCacheRetention iterating ID: " + id);

            var entries = store.getItemSync(id);
            var newEntries = [];
            for (var e in entries) {
                if (!entries[e].stored || ((entries[e].ts*1000) > retentionTime.getTime())) {
                    //copy array
                    newEntries.push(entries[e]);
                    //adapter.log.debug("checkCacheRetention entry copied: " + JSON.stringify(entries[e]));
                } else {
                    adapter.log.debug("checkCacheRetention Entry removed from Cache: " + JSON.stringify(entries[e]));
                }
            }
            store.setItem(id, newEntries);
            next();
        }).error(function (err) {
            adapter.log.error("checkCacheRetention error iterating ids: " + err.stack);
            runningprocess = false;
        }).end(function () {
            adapter.log.debug("checkCacheRetention iterating ended");
            runningprocess = false;
            store.persistSync();
        });
    }
}

function getDataFromDB(db, options, client, callback) {
    var query = SQLFuncs.getHistory(db, options);
    adapter.log.debug("getDataFromDB: " + query);

    client.execute(query, function (err, rows, fields) {
        if (rows && rows.rows) rows = rows.rows;
        // because descending
        if (rows) {
            for (var c = 0; c < rows.length; c++) {
                // todo change it after ms are added
                if (adapter.common.loglevel == 'debug') {
                    rows[c].date = new Date(parseInt(rows[c].ts, 10)*1000);
                }

                //quickfix for old values in ms
                while (rows[c].ts > 9000000000) {
                    rows[c].ts = Math.round(rows[c].ts / 1000);
                }

                if (options.ack) rows[c].ack = !!rows[c].ack;
                if (adapter.config.round) rows[c].val = Math.round(rows[c].val * adapter.config.round) / adapter.config.round;
                if (sqlDPs[options.index].type === 2) rows[c].val = !!rows[c].val;
            }
        }

        if (callback) callback(err, rows);
    });
}

function getCachedData(options, callback) {
    var cache = [];

    var res = store.getItemSync(options.id);
    cache = [];
    // todo can be optimized
    if (res) {
//        res = JSON.parse(JSON.stringify(states));
        var iProblemCount = 0;
        for (var i = res.length - 1; i >= 0 ; i--) {
            if (!res[i]) {
                iProblemCount++;
                continue;
            }
            if (options.start && res[i].ts < options.start) {
                break;
            } else if (res[i].ts > options.end) {
                continue;
            }
            if (options.ack) res[i].ack = !!res[i];

            cache.unshift(res[i]);

            if (!options.start && cache.length >= options.count) {
                break;
            }
        }
        if (iProblemCount) adapter.log.warn('got null states ' + iProblemCount + ' times for ' + options.id);

        adapter.log.debug('got ' + res.length + ' datapoints for ' + options.id);
    } else {
        adapter.log.debug('datapoints for ' + options.id + ' do not yet exist');
    }

    options.length = cache.length;

    callback(cache, !options.start && cache.length >= options.count);
}

function getHistory(msg) {
    var startTime = new Date().getTime();
    adapter.log.debug("timetrace start: " + (new Date().getTime() - startTime) + 'ms');
    var options = {
        id:         msg.message.id == '*' ? null : msg.message.id,
        start:      msg.message.options.start,
        end:        msg.message.options.end || Math.round((new Date()).getTime() / 1000) + 5000,
        step:       parseInt(msg.message.options.step) || null,
        count:      parseInt(msg.message.options.count) || 500,
        ignoreNull: msg.message.options.ignoreNull,
        aggregate:  msg.message.options.aggregate || 'average', // One of: max, min, average, total
        limit:      msg.message.options.limit || adapter.config.limit || 2000,
        from:       msg.message.options.from  || false,
        q:          msg.message.options.q     || false,
        ack:        msg.message.options.ack   || false,
        ms:         msg.message.options.ms    || false
    };

    var params = {
        server: adapter.config.host + (adapter.config.port ? ':' + adapter.config.port : ''),
        host: adapter.config.host + (adapter.config.port ? ':' + adapter.config.port : ''),
        user: adapter.config.user,
        password: adapter.config.password,
        max_idle: (adapter.config.dbtype === 'sqlite') ? 1 : 2
    };

    if (!sqlDPs[options.id]) {
        commons.sendResponse(adapter, msg, options, [], startTime);
        return;
    }
    if (!dbAvailable) {
        adapter.log.warn("DB is not available - returning cached data");
        adapter.log.debug("timetrace start getting from cache: " + (new Date().getTime() - startTime) + 'ms');

        if (options.start > options.end) {
            var _end      = options.end;
            options.end   = options.start;
            options.start = _end;
        }

        getCachedData(options, function (cacheData, isFull) {
            // if all data read
            adapter.log.debug("timetrace got all data: " + (new Date().getTime() - startTime) + 'ms');
            commons.sendResponse(adapter, msg, options, cacheData, startTime);
            adapter.log.debug("timetrace response sent: " + (new Date().getTime() - startTime) + 'ms - for ' + cacheData.length + ' entries');
        });

        // else: DB is available
    } else {
        adapter.log.debug("timetrace start getting from DB: " + (new Date().getTime() - startTime) + 'ms');
        clientPool.borrow(function (err, client) {
            if (err) {
                adapter.log.error(err);
                return;
            }
            adapter.log.debug("getHistory client connected " + client.threadId);

            if (options.start > options.end) {
                var _end = options.end;
                options.end = options.start;
                options.start = _end;
            }

            if (!options.start && !options.count) {
                options.start = Math.round((new Date()).getTime() / 1000) - 5030; // - 1 year
            }

            adapter.log.debug("timetrace getID: " + (new Date().getTime() - startTime) + 'ms');
            if (options.id && sqlDPs[options.id].index === undefined) {
                // read or create in DB

                return getId(options.id, null, client, function (err) {
                    if (err) {
                        adapter.log.warn('Cannot get index of "' + options.id + '": ' + err);
                        commons.sendResponse(adapter, msg, options, [], startTime);
                    } else {
                        getHistory(msg, client);
                    }
                    adapter.log.debug("timetrace got the ID: " + (new Date().getTime() - startTime) + 'ms');
                });
            }


            var type = sqlDPs[options.id].type;
            if (options.id) {
                options.index = options.id;
                options.id = sqlDPs[options.id].index;
            }

            // if specific id requested
            if (options.id || options.id === 0) {
                getDataFromDB(dbNames[type], options, client, function (err, data) {
                    //adapter.log.debug("data: " + JSON.stringify(data));
                    commons.sendResponse(adapter, msg, options, (err ? err.toString() : null) || data, startTime);
                    adapter.log.debug("timetrace response sent1: " + (new Date().getTime() - startTime) + 'ms - for ' + data.length + ' entries');
                });
            } else {
                // if all IDs requested
                var rows = [];
                var count = 0;
                for (var db = 0; db < dbNames.length; db++) {
                    count++;
                    getDataFromDB(dbNames[db], options, client, function (err, data) {
                        if (data) rows = rows.concat(data);
                        if (!--count) {
                            //rows.sort(sortByTs);
                            commons.sendResponse(adapter, msg, options, rows, startTime);
                            adapter.log.debug("timetrace response sent2: " + (new Date().getTime() - startTime) + 'ms - for ' + rows.length + ' entries');
                        }
                    });
                }
            }
            clientPool.return(client);
        });
    }
}

function generateDemo(msg) {
    var id      = adapter.name +'.' + adapter.instance + '.Demo.' + (msg.message.id || "Demo_Data");
    var start   = new Date(msg.message.start).getTime();
    var end     = new Date(msg.message.end).getTime();
    var value   = 1;
    var sin     = 0.1;
    var up      = true;
    var curve   = msg.message.curve;
    var step    = (msg.message.step || 60) * 1000;


    if (end < start) {
        var tmp = end;
        end = start;
        start = tmp;
    }

    end = new Date(end).setHours(24);

    function generate() {
        if (curve == 'sin') {
            if (sin == 6.2) {
                sin = 0;
            } else {
                sin = Math.round((sin + 0.1) * 10) / 10;
            }
            value = Math.round(Math.sin(sin) * 10000) / 100;
        } else if (curve == 'dec') {
            value++
        } else if (curve == 'inc') {
            value--;
        } else {
            if (up == true) {
                value++;
            } else {
                value--;
            }
        }
        start += step;

        insertStateIntoDB(id, {
            ts:   new Date(start).getTime() / 1000,
            val:  value,
            q:    0,
            ack:  true
        });


        if (start <= end) {
            setTimeout(function () {
                generate();
            }, 15)
        } else {
            adapter.sendTo(msg.from, msg.command, 'finished', msg.callback);
        }
    }
    var obj = {
        type: 'state',
        common: {
            name:       msg.message.id,
            type:       'state',
            enabled:    false,
            history:    {}
        }
    };
    obj.common.history[adapter.namespace] = {
        enabled:        true,
        changesOnly:    false,
        debounce:       1000,
        retention:      31536000
    };


    adapter.setObject('demo.' + msg.message.id, obj);

    sqlDPs[id] = {};
    sqlDPs[id][adapter.namespace] = obj.common.history[adapter.namespace];

    generate()
}

process.on('uncaughtException', function(err) {
    adapter.log.error('uncaught Exception in SQL-Adapter: ' + err.stack);
});