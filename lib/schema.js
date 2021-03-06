var types = require('./types');

function Schema(connection, description, opts) {
  this.connection = connection;

  var tableNameParts = [description.tableName];
  if (opts.tableNamePrefix) {
    tableNameParts.unshift(opts.tableNamePrefix);
  }
  if (opts.tableNameSuffix) {
    tableNameParts.push(opts.tableNameSuffix);
  }
  this._tableName = tableNameParts.join('-');
  this.key = description.key;
  this.attributes = description.attributes;
  this.expected = description.expected;
  this.searchable = description.searchable || Object.keys(description.attributes);
}

Schema.prototype.tableName = function(name) {
  if (name) {
    this._tableName = name;
    return this;
  }

  return this._tableName;
};

Schema.prototype.createTable = function(done) {
  this.connection.createTable({
    AttributeDefinitions: mapAttributes.call(this),
    KeySchema: mapKey.call(this),
    ProvisionedThroughput: {
      ReadCapacityUnits: 1,
      WriteCapacityUnits: 1
    },
    TableName: this.tableName()
  }, function(err, res) {
    done(err, res);
  });
};

Schema.prototype.describeTable = function(done) {
  this.connection.describeTable({
    TableName: this.tableName()
  }, done);
};

Schema.prototype.deleteTable = function(done) {
  this.connection.deleteTable({
    TableName: this.tableName()
  }, done);
};

function mapAttributes() {
  return this.key
    .map(function(key) {
      return {
        AttributeName: key,
        AttributeType: attributeType(this.attributes[key])
      };
    }.bind(this));
}

function mapKey() {
  return this.key
    .map(function(key, idx) {
      return {
        AttributeName: key,
        KeyType: idx === 0 ? 'HASH' : 'RANGE'
      };
    }.bind(this));
}

function attributeType(type) {
  switch (type) {
    case Number:
      return 'N';
    case String:
      return 'S';
    case Buffer:
      return 'B';
    case Boolean:
      return 'BOOL';
    case types.StringSet:
      return 'SS';
    case types.NumberSet:
      return 'NS';
    case types.BufferSet:
      return 'BS';
    case types.List:
      return 'L';
    case types.Map:
      return 'M';
    case Date:
      return 'S';
    default:
      throw(type + ' is not a matching type');
  }
}

module.exports = Schema;
