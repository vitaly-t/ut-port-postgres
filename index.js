var pg = require('pg');
var Port = require('ut-bus/port');
var errors = require('./errors');
var util = require('util');
var crypto = require('./crypto');
var queries = require('./sql');
var promisify = require('es6-promisify');

function PostgreSqlPort() {
    Port.call(this);
    this.config = {
        id: null,
        logLevel: '',
        type: 'postgresql',
        retry: 10000
    };
    this.client = null;
    this.retryTimeout = null;
}

util.inherits(PostgreSqlPort, Port);

PostgreSqlPort.prototype.init = function init() {
    Port.prototype.init.apply(this, arguments);
    this.latency = this.counter && this.counter('average', 'lt', 'Latency');
};

PostgreSqlPort.prototype.connect = function connect() {
    this.client && this.client.end();
    return Promise.resolve()
        .then(() => {
            if (this.config.db.cryptoAlgorithm) {
                return crypto.decrypt(this.config.db.password, this.config.db.cryptoAlgorithm)
                    .then((password) => (this.config.db.password = password));
            }
        })
        .then(this.tryConnect.bind(this))
        .then(this.loadSchema.bind(this))
        .then(this.linkFN.bind(this))
        .catch((err) => {
            this.client && this.client.end();
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
    this.client && this.client.end();
    Port.prototype.stop.apply(this, Array.prototype.slice.call(arguments));
};

PostgreSqlPort.prototype.checkConnection = function checkConnection(checkReady) {
    if (!this.client) {
        throw errors.noConnection({
            host: this.config.db && this.config.db.host,
            database: this.config.db && this.config.db.database
        });
    }
    if (checkReady && !this.connectionReady) {
        throw errors.notReady({
            host: this.config.db && this.config.db.host,
            database: this.config.db && this.config.db.database
        });
    }
};

PostgreSqlPort.prototype.tryConnect = function tryConnect() {
    this.connectionReady = false;
    this.client = new pg.Client(this.config.db);
    return promisify((cb) => {
        if (this.config.create) {
            var conCreate = new pg.Client({
                host: this.config.db.host,
                database: 'postgres',
                user: this.config.create.user,
                password: this.config.create.password
            });
            var endConCreate = promisify(conCreate.end.bind(conCreate));
            return promisify(conCreate.connect.bind(conCreate))()
                .then(() => conCreate.query(queries.createDatabase(this.config.db.database)))
                .then(() => conCreate.query(queries.createUser(this.config.db.database, this.config.db.user, this.config.db.password)))
                .catch((err) => {
                    return endConCreate()
                        .then(() => {
                            throw err;
                        });
                })
                .then(() => endConCreate);
        }
        cb(null);
    })()
    .then(() => promisify(this.client.connect.bind(this.client))())
    .then(() => {
        this.connectionReady = true;
        return this.client;
    });
};

PostgreSqlPort.prototype.loadSchema = function() {
    return this.client.query(queries.getFunctions())
        .then((result) => {
            return result.rows;
        });
};

PostgreSqlPort.prototype.updateSchema = function(schema) {
    return Promise.resolve(schema);
};

PostgreSqlPort.prototype.linkFN = function(schema) {
    schema.forEach((fn) => {
        this.config[fn.name] = (msg, $meta) => {
            return this.client.query(`SELECT * FROM "${fn.name}"()`)
                .then((result) => result.rows);
        };
    });
    return Promise.resolve();
};

PostgreSqlPort.prototype.exec = function(msg) {
    this.checkConnection(true);
    var $meta = arguments.length && arguments[arguments.length - 1];
    $meta.mtid = 'response';
    if (this.config[$meta.method]) {
        return this.config[$meta.method](msg, $meta);
    } else if (msg.query) {
        return this.client.query(msg.query)
            .then((result) => result.rows);
    }
    return Promise.reject(errors.missingQuery({
        msg: msg
    }));
};

module.exports = PostgreSqlPort;
