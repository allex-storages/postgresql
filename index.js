function main (execlib){
  'use strict';
  return execlib.loadDependencies('client', ['allex:storageregistry:lib', 'allex:basesql:storage', 'allex:postgresqlexecutor:lib'], createMSSQLStorage.bind(null, execlib));
}

function createMSSQLStorage (execlib, storagereglibignored, BaseSQLStorage, pgsqllib) {
  'use strict';

  function MSSQLStorage(storagedescriptor){
    BaseSQLStorage.call(this, storagedescriptor);
  }
  BaseSQLStorage.inherit(MSSQLStorage, pgsqllib);
  MSSQLStorage.prototype.expectedPrimaryKeyViolation = 'duplicate key value violates unique constraint';
  
  return MSSQLStorage;
}

module.exports = main;