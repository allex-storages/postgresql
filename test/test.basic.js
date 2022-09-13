var pgsql = require('pg');
var script = "DROP TABLE IF EXISTS users; "+
  "CREATE TABLE IF NOT EXISTS users ("+
  "name varchar(50) NOT NULL,"+
  "gender varchar(50) NOT NULL,"+
  "age int NOT NULL);"

var prophash = require('./prophash');

describe('Test Integration', function () {
  loadMochaIntegration('allex_datalib');
  it ('Load mssql storage', function () {
    return setGlobal('PostgreSQLStorageClass', require('..')(execlib));
  });
  it ('Create the "users" table first', function () {
    var pool = new pgsql.Pool(prophash);
    return qlib.promise2console(pool.query(script).then(
      function (res) {
        pool.end();
        return res;
      },
      function (reason) {
        pool.end();
        throw reason;
      }
    ), 'init');
  });
  for (var i=0; i<1; i++) {
    BasicStorageTest(
      function () { return PostgreSQLStorageClass; },
      function () { return prophash; }
    );
  }
});
