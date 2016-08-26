var pgp = require('pg-promise')();
var Port = require('ut-bus/port');
var errors = require('./errors');
var util = require('util');
var fs = require('fs');
var when = require('when');
var crypto = require('./crypto');
var uterror = require('ut-error');
var queries = require('./sql');

const AUDIT_LOG = /^[\s+]{0,}--ut-audit-params$/m;
const CORE_ERROR = /^[\s+]{0,}EXEC \[?core\]?\.\[?error\]?$/m;
const CALL_PARAMS = /^[\s+]{0,}DECLARE @callParams XML$/m;

function PostgreSqlPort() {
    Port.call(this);
    this.config = {
        id: null,
        logLevel: '',
        type: 'postgres',
        createTT: false,
        retry: 10000,
        tableToType: {},
        paramsOutName: 'out',
        doc: false
    };
    this.super = {};
    this.connection = null;
    this.retryTimeout = null;
}

function fieldSource(column) {
    return (column.column + '\t' +
        column.type + '\t' + (column.length === null ? '' : column.length) + '\t' + (column.scale === null ? '' : column.scale)).toLowerCase();
}

util.inherits(PostgreSqlPort, Port);

PostgreSqlPort.prototype.init = function init() {
    Port.prototype.init.apply(this, arguments);
    this.latency = this.counter && this.counter('average', 'lt', 'Latency');
};

PostgreSqlPort.prototype.connect = function connect() {
    this.connection && this.connection.end();
    this.connectionReady = false;

    return Promise.resolve()
        .then(() => {
            if (this.config.db.cryptoAlgorithm) {
                return crypto.decrypt(this.config.db.password, this.config.db.cryptoAlgorithm)
                    .then((password) => (this.config.db.password = password));
            }
        })
        .then(this.tryConnect.bind(this))
        .then(this.loadSchema.bind(this))
        .then(this.updateSchema.bind(this))
        .then(this.linkSP.bind(this))
        .then((v) => { this.connectionReady = true; return v; })
        .catch((err) => {
            this.connection = null;
            if (this.config.retry) {
                this.retryTimeout = setTimeout(this.connect.bind(this), this.config.retry || 10000);
                this.log.error && this.log.error(err);
            } else {
                throw err;
            }
        });
};

PostgreSqlPort.prototype.start = function start() {
    this.bus && this.bus.importMethods(this.config, this.config.imports, undefined, this);
    return Port.prototype.start.apply(this, Array.prototype.slice.call(arguments))
        .then(this.connect.bind(this))
        .then((result) => {
            this.pipeExec(this.exec.bind(this), this.config.concurrency);
            return result;
        });
};

PostgreSqlPort.prototype.stop = function stop() {
    this.retryTimeout && clearTimeout(this.retryTimeout);
    // this.queue.push();
    this.connectionReady = false;
    this.connection = null;
    Port.prototype.stop.apply(this, Array.prototype.slice.call(arguments));
};

function setPathProperty(object, fieldName, fieldValue) {
    var path = fieldName.split('.');
    fieldName = path.pop();
    path.forEach(function(name) {
        if (name) {
            if (!(object[name] instanceof Object)) {
                object[name] = {};
            }
            object = object[name];
        }
    });
    object[fieldName] = fieldValue;
}

PostgreSqlPort.prototype.checkConnection = function(checkReady) {
    if (!this.connection) {
        throw errors.noConnection({
            server: this.config.db && this.config.db.host,
            database: this.config.db && this.config.db.database
        });
    }
    if (checkReady && !this.connectionReady) {
        throw errors.notReady({
            server: this.config.db && this.config.db.host,
            database: this.config.db && this.config.db.database
        });
    }
};

PostgreSqlPort.prototype.tryConnect = function() {
    if (this.config.create) {
        var conCreate = pgp({
            host: this.config.db.host,
            database: 'postgres',
            user: this.config.create.user,
            password: this.config.create.password
        });
        var req;
        return conCreate.connect()
        .then((con) => { req = con; })
        .then(() => (req.query(queries.createDatabase(this.config.db.database))))
        .then(() => (req.query(queries.createUser(this.config.db.database, this.config.db.user, this.config.db.password))))
        .then(() => req.done())
        .then(() => {
            this.connection = pgp(this.config.db);
            return this.connection;
        })
        .catch((err) => {
            try { req && req.done(); } catch (e) {};
            throw err;
        });
    } else {
        this.connection = pgp(this.config.db);
        return this.connection;
    }
};

PostgreSqlPort.prototype.loadSchema = function(objectList) {
    var self = this;
    var schema = this.getSchema();
    if ((Array.isArray(schema) && !schema.length) || !schema) {
        return {source: {}, parseList: []};
    }

    this.checkConnection();
    return this.getRequest()
        .then((request) => {
            return request.query(queries.loadSchema()).then(function(result) {
                var schema = {source: {}, parseList: [], types: {}, deps: {}};
                result.reduce(function(prev, cur) { // extract source code of procedures, views, functions, triggers
                    cur.namespace = cur.namespace && cur.namespace.toLowerCase();
                    cur.full = cur.full && cur.full.toLowerCase();
                    if (cur.source) {
                        prev.source[cur.full] = (prev.source[cur.full] || '') + (cur.source || '');
                    } else if (cur.full) {
                        prev.source[cur.full] = '';
                    } else {
                        prev.source[cur.namespace] = '';
                    }
                    if ((cur.type === 'P') && (self.config.linkSP || (objectList && objectList[cur.full]))) {
                        prev.parseList.push({
                            source: cur.source,
                            params: cur.params, name: '"' + cur.namespace + '"."' + cur.name + '"',
                            fileName: objectList && objectList[cur.full]
                        });
                    }
                    return prev;
                }, schema);
                // result[1].reduce(function(prev, cur) { // extract columns of user defined table types
                //     var parserDefault = require('./parsers/mssqlDefault');
                //     if (!(mssql[cur.type.toUpperCase()] instanceof Function)) {
                //         throw errors.unexpectedColumnType({
                //             type: cur.type,
                //             userDefinedTableType: cur.name
                //         });
                //     }
                //     cur.name = cur.name && cur.name.toLowerCase();
                //     cur.default && (cur.default = parserDefault.parse(cur.default));
                //     var type = prev[cur.name] || (prev[cur.name] = []);
                //     type.push(cur);
                //     return prev;
                // }, schema.types);
                // result[2].reduce(function(prev, cur) { // extract dependencies
                //     cur.name = cur.name && cur.name.toLowerCase();
                //     cur.type = cur.type && cur.type.toLowerCase();
                //     var dep = prev[cur.type] || (prev[cur.type] = {names: [], drop: []});
                //     if (dep.names.indexOf(cur.name) < 0) {
                //         dep.names.push(cur.name);
                //         dep.drop.push(cur.drop);
                //     }
                //     return prev;
                // }, schema.deps);
                Object.keys(schema.types).forEach(function(type) { // extract pseudo source code of user defined table types
                    schema.source[type] = schema.types[type].map(fieldSource).join('\r\n');
                });
                return schema;
            });
        });
};

PostgreSqlPort.prototype.getSchema = function() {
    var result = [];
    if (this.config.schema) {
        if (Array.isArray(this.config.schema)) {
            result = this.config.schema.slice();
        } else {
            result.push({path: this.config.schema});
        }
    }
    this.config.imports && this.config.imports.forEach(function(imp) {
        imp.match(/\.schema$/) && Array.prototype.push.apply(result, this.config[imp]);
        this.config[imp + '.schema'] && Array.prototype.push.apply(result, this.config[imp + '.schema']);
    }.bind(this));
    return result;
};

PostgreSqlPort.prototype.updateSchema = function(schema) {
    this.checkConnection();

    function replaceAuditLog(statement) {
        var parserSP = require('./parsers/postgres');
        var binding = parserSP.parse(statement);
        return statement.trim().replace(AUDIT_LOG, queries.auditLog(binding));
    }

    function replaceCallParams(statement) {
        var parserSP = require('./parsers/postgres');
        var binding = parserSP.parse(statement);
        return statement.trim().replace(CALL_PARAMS, queries.callParams(binding));
    }

    function replaceCoreError(statement, fileName, objectName, params) {
        return statement
            .split('\n')
            .map((line, index) => (line.replace(CORE_ERROR,
                `DECLARE @CORE_ERROR_FILE sysname='${fileName.replace(/'/g, '\'\'')}' ` +
                `DECLARE @CORE_ERROR_LINE int='${index + 1}' ` +
                `EXEC [core].[errorStack] @procid=@@PROCID, @file=@CORE_ERROR_FILE, @fileLine=@CORE_ERROR_LINE, @params = ${params}`)))
            .join('\n');
    }

    function preProcess(statement, fileName, objectName) {
        if (statement.match(AUDIT_LOG)) {
            statement = replaceAuditLog(statement);
        }
        var params = 'NULL';
        if (statement.match(CALL_PARAMS)) {
            statement = replaceCallParams(statement);
            params = '@callParams';
        }
        if (statement.match(CORE_ERROR)) {
            statement = replaceCoreError(statement, fileName, objectName, params);
        }
        return statement;
    }

    function getAlterStatement(statement, fileName, objectName) {
        statement = preProcess(statement, fileName, objectName);
        if (statement.trim().match(/^(CREATE\s+TYPE)|(CREATE\s+OR\s+REPLACE\s)/i)) {
            return statement.trim();
        } else {
            return statement.trim().replace(/^CREATE /i, 'ALTER ');
        }
    }

    function tableToType(statement) {
        if (statement.match(/^CREATE\s+TABLE/i)) {
            var parserSP = require('./parsers/postgres');
            var binding = parserSP.parse(statement);
            if (binding.type === 'table') {
                var name = binding.name.match(/\]$/) ? binding.name.slice(0, -1) + 'TT]' : binding.name + 'TT';
                var columns = binding.fields.map(function(field) {
                    return `[${field.column}] [${field.type}]` +
                        (field.length !== null && field.scale !== null ? `(${field.length},${field.scale})` : '') +
                        (field.length !== null && field.scale === null ? `(${field.length})` : '') +
                        (typeof field.default === 'number' ? ` DEFAULT(${field.default})` : '') +
                        (typeof field.default === 'string' ? ` DEFAULT('${field.default.replace(/'/g, '\'\'')}')` : '');
                });
                return 'CREATE TYPE ' + name + ' AS TABLE (\r\n  ' + columns.join(',\r\n  ') + '\r\n)';
            }
        }
        return '';
    }

    function tableToTTU(statement) {
        var result = '';
        if (statement.match(/^CREATE\s+TABLE/i)) {
            var parserSP = require('./parsers/postgres');
            var binding = parserSP.parse(statement);
            if (binding.type === 'table') {
                var name = binding.name.match(/\]$/) ? binding.name.slice(0, -1) + 'TTU]' : binding.name + 'TTU';
                var columns = binding.fields.map(function(field) {
                    return ('[' + field.column + '] [' + field.type + ']' +
                        (field.length !== null && field.scale !== null ? `(${field.length},${field.scale})` : '') +
                        (field.length !== null && field.scale === null ? `(${field.length})` : '') +
                        ',\r\n' + field.column + 'Updated bit');
                });
                result = 'CREATE TYPE ' + name + ' AS TABLE (\r\n  ' + columns.join(',\r\n  ') + '\r\n)';
            }
        }
        return result;
    }

    function getCreateStatement(statement, fileName, objectName) {
        return preProcess(statement, fileName, objectName).trim().replace(/^ALTER /i, 'CREATE ');
    }

    function getSource(statement, fileName, objectName) {
        statement = preProcess(statement, fileName, objectName);
        if (statement.trim().match(/^CREATE\s+TYPE/i)) {
            var parserSP = require('./parsers/postgres');
            var binding = parserSP.parse(statement);
            if (binding && binding.type === 'table type') {
                return binding.fields.map(fieldSource).join('\r\n');
            }
        }
        return statement.trim().replace(/^ALTER /i, 'CREATE ');
    }

    function addQuery(queries, params) {
        if (schema.source[params.objectId] === undefined) {
            queries.push({
                fileName: params.fileName,
                objectName: params.objectName,
                objectId: params.objectId,
                content: params.createStatement
            });
        } else {
            if (schema.source[params.objectId].length &&
                (getSource(params.fileContent, params.fileName, params.objectName) !== schema.source[params.objectId])) {
                var deps = schema.deps[params.objectId];
                if (deps) {
                    deps.names.forEach(function(dep) {
                        delete schema.source[dep];
                    });
                    queries.push({
                        fileName: params.fileName,
                        objectName: params.objectName + ' drop dependencies',
                        objectId: params.objectId,
                        content: deps.drop.join('\r\n')
                    });
                }
                queries.push({
                    fileName: params.fileName,
                    objectName: params.objectName,
                    objectId: params.objectId,
                    content: getAlterStatement(params.fileContent, params.fileName, params.objectName)
                });
            }
        }
    }

    function getObjectName(fileName) {
        return fileName.replace(/\.sql$/i, '').replace(/^[^\$]*\$/, ''); // remove "prefix$" and ".sql" suffix
    }

    function shouldCreateTT(tableName) {
        if (self.config.createTT === true || self.config.tableToType[tableName] === true) {
            return true;
        }
        return false;
    }

    var self = this;
    var schemas = this.getSchema();
    if (!schemas) {
        return schema;
    }

    return when.reduce(schemas, function(prev, schemaConfig) { // visit each schema folder
        return when.promise(function(resolve, reject) {
            fs.readdir(schemaConfig.path, function(err, files) {
                if (err) {
                    reject(err);
                } else {
                    var queries = [];
                    files = files.sort();
                    var objectIds = files.reduce(function(prev, cur) {
                        prev[getObjectName(cur).toLowerCase()] = true;
                        return prev;
                    }, {});
                    files.forEach(function(file) {
                        var objectName = getObjectName(file);
                        var objectId = objectName.toLowerCase();
                        var fileName = schemaConfig.path + '/' + file;
                        schemaConfig.linkSP && (prev[objectId] = fileName);
                        var fileContent = fs.readFileSync(fileName).toString();
                        addQuery(queries, {
                            fileName: fileName,
                            objectName: objectName,
                            objectId: objectId,
                            fileContent: fileContent,
                            createStatement: getCreateStatement(fileContent, fileName, objectName)
                        });
                        if (shouldCreateTT(objectId) && !objectIds[objectId + 'tt']) {
                            var tt = tableToType(fileContent.trim().replace(/^ALTER /i, 'CREATE '));
                            if (tt) {
                                addQuery(queries, {
                                    fileName: fileName,
                                    objectName: objectName + 'TT',
                                    objectId: objectId + 'tt',
                                    fileContent: tt,
                                    createStatement: tt
                                });
                            }
                            var ttu = tableToTTU(fileContent.trim().replace(/^ALTER /i, 'CREATE '));
                            if (ttu) {
                                addQuery(queries, {
                                    fileName: fileName,
                                    objectName: objectName + 'TTU',
                                    objectId: objectId + 'ttu',
                                    fileContent: ttu,
                                    createStatement: ttu
                                });
                            }
                        }
                    });

                    var currentFileName = '';
                    var updated = [];
                    when.reduce(queries, function(result, query) {
                        updated.push(query.objectName);
                        currentFileName = query.fileName;
                        return self.getRequest().then((request) => request.query(query.content));
                    }, [])
                    .then(function() {
                        updated.length && self.log.info && self.log.info({
                            message: updated,
                            $meta: {
                                opcode: 'updateSchema'
                            }
                        });
                        resolve(prev);
                    })
                    .catch(function(error) {
                        error.fileName = currentFileName;
                        error.message = error.message + ' (' + currentFileName + ':' + (error.lineNumber || 1) + ':1)';
                        reject(error);
                    });
                }
            });
        }, []);
    }, [])
    .then(function(objectList) {
        return self.loadSchema(objectList);
    });
};

PostgreSqlPort.prototype.getRequest = function() {
    return this.connection.connect();
};

PostgreSqlPort.prototype.callSP = function(name, params, flatten, fileName) {
    var self = this;
    var outParams = [];

    params && params.forEach(function(param) {
        param.out && outParams.push(param.name);
    });

    // function sqlType(def) {
    //     var type;
    //     if (def.type === 'table') {
    //         type = def.create();
    //     } else {
    //         type = mssql[def.type.toUpperCase()];
    //     }
    //     if (def.size) {
    //         if (Array.isArray(def.size)) {
    //             type = type(def.size[0], def.size[1]);
    //         } else {
    //             type = (def.size === 'max') ? type(mssql.MAX) : type(def.size);
    //         }
    //     }
    //     return type;
    // }

    function flattenMessage(data, delimiter) {
        if (!delimiter) {
            return data;
        }
        var result = {};
        function recurse(cur, prop) {
            if (Object(cur) !== cur) {
                result[prop] = cur;
            } else if (Array.isArray(cur)) {
                // for (var i = 0, l = cur.length; i < l; i += 1) {
                //     recurse(cur[i], prop + '[' + i + ']');
                // }
                // if (l === 0) {
                //     result[prop] = [];
                // }
                result[prop] = cur;
            } else {
                var isEmpty = true;
                for (var p in cur) {
                    isEmpty = false;
                    recurse(cur[p], prop ? prop + delimiter + p : p);
                }
                if (isEmpty && prop) {
                    result[prop] = {};
                }
            }
        }
        recurse(data, '');
        return result;
    }
    // function getValue(column, value, def, updated) {
    //     if (updated) {
    //         return updated;
    //     }
    //     if (value === undefined) {
    //         return def;
    //     } else if (value) {
    //         if (/^(date.*|smalldate.*)$/.test(column.type.declaration)) {
    //             // set a javascript date for 'date', 'datetime', 'datetime2' 'smalldatetime' and 'time'
    //             return new Date(value);
    //         } else if (column.type.declaration === 'time') {
    //             return new Date('1970-01-01T' + value);
    //             // } else if (column.type.declaration === 'xml') {
    //             //     var obj = {};
    //             //     obj[column.name] = value;
    //             //     return xmlBuilder.buildObject(obj);
    //         }
    //     }
    //     return value;
    // }
    return function callLinkedSP(msg, $meta) {
        self.checkConnection(true);
        var data = flattenMessage(msg, flatten);
        var debug = this.isDebug();
        var debugParams = {};
        var values = [];
        params && params.forEach(function(param) {
            var value;
            if (param.name === 'meta') {
                value = $meta;
            } else if (param.update) {
                value = data[param.name] || data.hasOwnProperty(param.update);
            } else {
                value = data[param.name];
            }
            // var hasValue = value !== void 0;
            debug && (debugParams[param.name] = value);
            if (param.def && param.def.type === 'time') {
                value = new Date('1970-01-01T' + value);
            }
            values.push(value);
            // var typeXX = sqlType(param.def);
            // if (param.out) {
            //     request.output(param.name, type, value);
            // } else {
            //     if (param.def && param.def.type === 'table') {
            //         if (value) {
            //             if (Array.isArray(value)) {
            //                 value.forEach(function(row) {
            //                     row = flattenMessage(row, param.flatten);
            //                     if (typeof row === 'object') {
            //                         type.rows.add.apply(type.rows, param.columns.reduce(function(prev, cur, i) {
            //                             prev.push(getValue(type.columns[i], row[cur.column], cur.default, cur.update && row.hasOwnProperty(cur.update)));
            //                             return prev;
            //                         }, []));
            //                     } else {
            //                         type.rows.add.apply(type.rows, [getValue(type.columns[0], row, param.columns[0].default, false)]
            //                             .concat(new Array(param.columns.length - 1)));
            //                     }
            //                 });
            //             } else if (typeof value === 'object') {
            //                 value = flattenMessage(value, param.flatten);
            //                 type.rows.add.apply(type.rows, param.columns.reduce(function(prev, cur, i) {
            //                     prev.push(getValue(type.columns[i], value[cur.column], cur.default, cur.update && value.hasOwnProperty(cur.update)));
            //                     return prev;
            //                 }, []));
            //             } else {
            //                 value = flattenMessage(value, param.flatten);
            //                 type.rows.add.apply(type.rows, [getValue(type.columns[0], value, param.columns[0].default, false)]
            //                     .concat(new Array(param.columns.length - 1)));
            //             }
            //         }
            //         request.input(param.name, type);
            //     } else {
            //         if (!param.default || hasValue) {
            //             request.input(param.name, type, value);
            //         }
            //     }
            // }
        });
        return self.getRequest().then((request) => request.func(name, values)
            .then(function(resultsets) {
                function isNamingResultSet(element) {
                    return Array.isArray(element) &&
                        element.length === 1 &&
                        element[0].hasOwnProperty('resultSetName') &&
                        typeof element[0].resultSetName === 'string';
                }

                if (resultsets.length > 0 && isNamingResultSet(resultsets[0])) {
                    var namedSet = {};
                    if (outParams.length) {
                        namedSet[self.config.paramsOutName] = outParams.reduce(function(prev, curr) {
                            prev[curr] = request.parameters[curr].value;
                            return prev;
                        }, {});
                    }
                    var name = null;
                    var single = false;
                    for (var i = 0; i < resultsets.length; ++i) {
                        if (name == null) {
                            if (!isNamingResultSet(resultsets[i])) {
                                throw errors.invalidResultSetOrder({
                                    expectName: true
                                });
                            } else {
                                name = resultsets[i][0].resultSetName;
                                single = !!resultsets[i][0].single;
                            }
                        } else {
                            if (isNamingResultSet(resultsets[i])) {
                                throw errors.invalidResultSetOrder({
                                    expectName: false
                                });
                            }
                            if (namedSet.hasOwnProperty(name)) {
                                throw errors.duplicateResultSetName({
                                    name: name
                                });
                            }
                            if (single) {
                                if (resultsets[i].length === 0) {
                                    namedSet[name] = null;
                                } else if (resultsets[i].length === 1) {
                                    namedSet[name] = resultsets[i][0];
                                } else {
                                    throw errors.singleResultExpected({
                                        count: resultsets[i].length
                                    });
                                }
                            } else {
                                namedSet[name] = resultsets[i];
                            }
                            name = null;
                            single = false;
                        }
                    }
                    if (name != null) {
                        throw errors.invalidResultSetOrder({
                            expectName: false
                        });
                    }
                    return namedSet;
                }
                if (outParams.length) {
                    resultsets.push([outParams.reduce(function(prev, curr) {
                        prev[curr] = request.parameters[curr].value;
                        return prev;
                    }, {})]);
                }
                if (resultsets && resultsets.length === 1 && resultsets[0].isSingleResult) {
                    delete resultsets[0].isSingleResult;
                    return resultsets[0];
                } else {
                    return resultsets;
                }
            })
            .catch(function(err) {
                var errorLines = err.message && err.message.split('\n');
                err.message = errorLines.shift();
                var error = uterror.get(err.message) || errors.sql;
                var errToThrow = error(err);
                if (debug) {
                    err.storedProcedure = name;
                    err.params = debugParams;
                    err.fileName = fileName + ':1:1';
                    var stack = errToThrow.stack.split('\n');
                    stack.splice.apply(stack, [1, 0].concat(errorLines));
                    errToThrow.stack = stack.join('\n');
                }
                throw errToThrow;
            })
        );
    };
};

PostgreSqlPort.prototype.linkSP = function(schema) {
    if (schema.parseList.length) {
        // var parserSP = require('./parsers/postgres');
        schema.parseList.forEach(function(procedure) {
            // var binding = parserSP.parse(procedure.source);
            var binding = {
                name: procedure.name,
                type: 'function',
                params: procedure.params.split(', ').map((param) => {
                    return {
                        name: param.split(' ')[0].replace(/[@"]/g, ''),
                        def: {
                            type: param.split(' ')[1]
                        }
                    };
                })
            };
            var flatName = binding.name.replace(/["]/g, '');
            if (binding && binding.type === 'function') {
                var update = [];
                var flatten = false;
                binding.params && binding.params.forEach(function(param) {
                    update.push(param.name + '$update');
                    // flatten in case a parameter's name have at least one underscore character surrounded by non underscore characters
                    if (!flatten && param.name.match(/\./)) {
                        flatten = '.';
                    } else if (!flatten && param.name.match(/[^_]_[^_]/)) {
                        flatten = '_';
                    }
                });
                binding.params && binding.params.forEach(function(param) {
                    (update.indexOf(param.name) >= 0) && (param.update = param.name.replace(/\$update$/i, ''));
                    if (param.def && param.def.type === 'table') {
                        var columns = schema.types[param.def.typeName.toLowerCase()];
                        param.columns = [];
                        param.flatten = false;
                        columns.forEach(function(column) {
                            if (column.column.match(/Updated$/)) {
                                column.update = column.column.replace(/Updated$/, '');
                            }
                            param.columns.push(column);
                            if (column.column.match(/\./)) {
                                param.flatten = '.';
                            }
                        });
                        // param.def.create = function() {
                        //     var table = new mssql.Table(param.def.typeName.toLowerCase());
                        //     columns && columns.forEach(function(column) {
                        //         var type = mssql[column.type.toUpperCase()];
                        //         if (!(type instanceof Function)) {
                        //             throw errors.unexpectedType({
                        //                 type: column.type,
                        //                 procedure: binding.name
                        //             });
                        //         }
                        //         if (typeof column.length === 'string' && column.length.match(/^max$/i)) {
                        //             table.columns.add(column.column, type(mssql.MAX));
                        //         } else {
                        //             table.columns.add(column.column, type(column.length !== null ? Number.parseInt(column.length) : column.length, column.scale));
                        //         }
                        //     });
                        //     return table;
                        // };
                    }
                });
                this.super[flatName] = this.callSP(binding.name, binding.params, flatten, procedure.fileName).bind(this);
                if (!this.config[flatName]) {
                    this.config[flatName] = this.super[flatName];
                }
            }
        }.bind(this));
    }
    return schema;
};

PostgreSqlPort.prototype.exec = function(message) {
    var $meta = (arguments.length && arguments[arguments.length - 1]);
    var methodName = ($meta && $meta.method);
    if (methodName) {
        var method = this.config[methodName];
        if (!method) {
            methodName = methodName.split('/', 2);
            method = methodName.length === 2 && this.config[methodName[1]];
        }
        if (method instanceof Function) {
            return when.lift(method).apply(this, Array.prototype.slice.call(arguments));
        }
    }

    this.checkConnection(true);

    if (this.config.validate instanceof Function) {
        this.config.validate(message);
    }

    // var start = +new Date();
    var debug = this.isDebug();
    return this.getRequest().then((request) =>
        when.promise(function(resolve, reject) {
            request.query(message.query, function(err, result) {
                // var end = +new Date();
                // var execTime = end - start;
                // todo record execution time
                if (err) {
                    debug && (err.query = message.query);
                    var error = uterror.get(err.message && err.message.split('\n').shift()) || errors.sql;
                    reject(error(err));
                } else {
                    $meta.mtid = 'response';
                    if (message.process === 'return') {
                        if (result && result.length) {
                            Object.keys(result[0]).forEach(function(value) {
                                setPathProperty(message, value, result[0][value]);
                            });
                        }
                        resolve(message);
                    } else if (message.process === 'json') {
                        message.dataSet = result;
                        resolve(message);
                    } else if (message.process === 'xls') { // todo
                        reject(errors.notImplemented(message.process));
                    } else if (message.process === 'xml') { // todo
                        reject(errors.notImplemented(message.process));
                    } else if (message.process === 'csv') { // todo
                        reject(errors.notImplemented(message.process));
                    } else if (message.process === 'processRows') { // todo
                        reject(errors.notImplemented(message.process));
                    } else if (message.process === 'queueRows') { // todo
                        reject(errors.notImplemented(message.process));
                    } else {
                        reject(errors.missingProcess(message.process));
                    }
                }
            });
        }));
};

module.exports = PostgreSqlPort;
