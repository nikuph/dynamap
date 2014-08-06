var rc      = require('rc');
var uuid    = require('node-uuid');
var dynamap = require('../lib/dynamap');

var conf = rc('dynamodb');

dynamap.initialize(conf);

var Animal = dynamap.Model({
  tableName: 'animals-test',
  key: ['id'],
  attributes: {
    id:     Number,
    animal: String,
    name:   String,
    born:   Number
  },
  expected: {
    id: {
      ComparisonOperator: 'NULL'
    }
  }
});

// Animal.schema.createTable(function(err, done) {
//   console.log(err, done);
// });

// Animal.schema.deleteTable(function(err, done) {
//   console.log(err, done);
// });

Animal.prototype.beforeSave = function(done) {
  if (this.id === undefined) {
    this.id = uuid.v1();
  }

  done();
};

function logSaving(err, animal) {
  if (err) {
    return console.error(err);
  }

  console.log('saved ', animal);
}

var animals = [
  new Animal({id: 1, animal: 'dog', name: 'susi',    born: new Date('2010-01-01').getTime()}),
  new Animal({id: 2, animal: 'dog', name: 'strolch', born: new Date('2011-01-01').getTime()}),
  new Animal({id: 3, animal: 'cat', name: 'susi',    born: new Date('2012-01-01').getTime()}),
  new Animal({id: 4, animal: 'cat', name: 'felix',   born: new Date('2013-01-01').getTime()}),
  new Animal({id: 5, animal: 'cat', name: 'ingo'})
];

// animals.forEach(function(animal) {
//   animal.save(logSaving);
// });

function logAnimalsWithPrefix(prefix) {
  return function(err, animals) {
    if (err) {
      return console.error(err);
    }

    animals.forEach(function(animal) {
      console.log(prefix, animal, animal instanceof Animal);
    });
  };
}

Animal.query({id: 3}, logAnimalsWithPrefix('querried: '));
Animal.scan('born LT ' + new Date('2012-01-01').getTime(), logAnimalsWithPrefix('scanned: '));
Animal.all(logAnimalsWithPrefix('all: '));
