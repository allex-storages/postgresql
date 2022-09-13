var pgsql = require('pg');
var script = "DROP TABLE IF EXISTS users; "+
  "CREATE TABLE users ("+
  "name varchar(50) NOT NULL,"+
  "gender varchar(50) NOT NULL,"+
  "age int NOT NULL);"

function prophashfunc () {
  return require('./prophash');
}
function recordDescriptor (primarykey, indices) {
  return {
    fields: [{
      name: 'name',
      type: 'string',
      sqltype: 'varchar(50)',
      nullable: false
    },{
      name: 'gender',
      type: 'string',
      sqltype: 'varchar(50)',
      nullable: false
    },{
      name: 'age',
      type: 'number',
      sqltype: 'int',
      nullable: false
    }],
    primaryKey: primarykey,
    indices: indices
  };
}

function columner (thingy) {
  if (lib.isArray(thingy)) {
    return thingy;
  }
  if (lib.isString(thingy)) {
    return [thingy];
  }
  throw new lib.Error('COLUMNS_NEITHER_AN_ARRAY_NOR_A_STRING', 'Columns provided to checkOneIndex have to be an Array of Strings or a single String');
}

function checkOneIndex (desc, columns) {
  if (!desc && !columns) {
    return;
  }
  if (!desc) {
    throw new lib.Error('NO_DESCRIPTOR', 'There is no descriptor');
  }
  columns = columner(columns);
  if (desc.matchesColumns(columns)) {
    return;
  }
  throw new lib.Error('INDEX_MISMATCH', 'Mismatch');
}

function resolutionReducer (idxs, res, columns) {
  var m = idxs.matchesColumnsOnNonPrimary(columns);
  res.push(m
    ?
    {
      code: 0,
      name: m
    }
    :
    {
      code: 2,
      columns: columns
    }
  )
  return res;
}

function okReducer (res, item) {
  if (item&&item.code==0&&item.name) {
    res.push(item.name);
  }
  return res;
}

function checkAllIndices (idxs, primarykey, indices) {
  var idxresolutions, resolved=[];
  checkOneIndex(idxs.primary, primarykey);
  if (lib.isArray(indices)) {
    idxresolutions = indices.reduce(resolutionReducer.bind(null, idxs), []);
    resolved = idxresolutions.reduce(okReducer, []);
  }
  if (resolved.length + (idxs.primary?1:0) == idxs.all.count) {
    return;
  }
  throw new lib.Error('INDEX_COUNT_MISMATCH', 'Expected '+idxs.all.count+' indexes, but got '+resolved.length + (idxs.primary?1:0));
}

function itsForPrimaryKeyAndIndices (title, primarykey, indices){
  it(lib.joinStringsWith(title, 'Instantiate a Spawning Manager', ': '), function () {
    this.timeout(1e7);
    console.log('StorageKlass', StorageKlass.name);
    return setGlobal('Manager', instantiateDataManager(prophashfunc, recordDescriptor(
      primarykey,
      indices
    )));
  });
  /*
  it(lib.joinStringsWith(title, 'Trivial read',': '), function () {
    return onerecordreader({});
  });
  */
  it(lib.joinStringsWith(title, 'Read indices',': '), function () {
    this.timeout(1e7);
    return setGlobal('Indices', Manager.storage.readIndices());
  });
  it(lib.joinStringsWith(title, 'Check indices',': '), function () {
    this.timeout(1e7);
    checkAllIndices(Indices, primarykey, indices);
  });
  it(lib.joinStringsWith(title, 'Destroy Spawning Manager',': '), function () {
    Manager.destroy();
  });
}


describe('Test Indices', function () {
  loadMochaIntegration('allex_datalib');
  if (!getGlobal('allex_datalib')) {
    loadClientSide(['allex:datafilters:lib', 'allex:data:lib']);
  }
  it ('Load pgsql storage', function () {
    return setGlobal('StorageKlass', require('..')(execlib));
  });
  it ('Create the "users" table', function () {
    var pool = new pgsql.Pool(prophashfunc());
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
  itsForPrimaryKeyAndIndices('Single PK, no indices', 'name');
  itsForPrimaryKeyAndIndices('2-segment PK, no indices', ['name', 'gender']);
  itsForPrimaryKeyAndIndices('Single PK, one index', 'name', ['gender']);
  itsForPrimaryKeyAndIndices('Single PK, two indices', 'name', ['gender', ['gender', 'age']]);
});