var create = require('ut-error').define;

var PortPostgreSQL = create('PortPostgreSQL');
var NoConnection = create('NoConnection', PortPostgreSQL);
var NotReady = create('NotReady', PortPostgreSQL);
var MissingQuery = create('MissingQuery', PortPostgreSQL);

module.exports = {
    sql: function(cause) {
        return new PortPostgreSQL(cause);
    },
    noConnection: function(params) {
        return new NoConnection({message: 'No connection to PostgreSQL server', params: params});
    },
    notReady: function(params) {
        return new NotReady({message: 'The connection is not ready', params: params});
    },
    missingQuery: function(params) {
        return new MissingQuery({message: 'Missing query', params: params});
    }
};
