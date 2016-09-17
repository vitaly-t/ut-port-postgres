'use strict';

var path = require('path');
var QueryFile = require('pg-promise').QueryFile;

function sql(file) {
    return new QueryFile(path.join(__dirname, file), {minify: true});
}

module.exports = {
    loadSchema: sql('load-schema.sql'),
    createDatabase: sql('create-database.sql'),
    createUser: sql('create-user.sql')
};
