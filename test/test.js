var rc      = require('rc');
var uuid    = require('node-uuid');
var dynamap = require('../lib/dynamap');

var conf = rc('dynamodb');

dynamap.initialize(conf);

var Animal = dynamap.Model({
  tableName: 'animals-test',
  key: ['animal', 'id'],
  attributes: {
    id:     String,
    animal: String,
    name:   String
  }
});

// Animal.schema.createTable(function(err, done) {
//   console.log(err, done);
// });

// Animal.schema.deleteTable(function(err, done) {
//   console.log(err, done);
// });

Animal.prototype.beforeSave = function(done) {
  this.id = uuid.v1();

  done();
};

function logSaving(err, animal) {
  if (err) {
    return console.error(err);
  }

  console.log('saved ', animal);
}

var animals = [
  new Animal({animal: 'dog', name: 'susi'}),
  new Animal({animal: 'dog', name: 'strolch'}),
  new Animal({animal: 'cat', name: 'susi'}),
  new Animal({animal: 'cat', name: 'felix'})
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

Animal.query({animal: 'cat'}, logAnimalsWithPrefix('querried: '));
Animal.scan('name EQ susi', logAnimalsWithPrefix('scanned: '));
Animal.all(logAnimalsWithPrefix('all: '));
