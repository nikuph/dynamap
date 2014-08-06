function Schema(connection, description) {
  this.connection = connection;

  this._tableName = description.tableName;
  this.key = description.key;
  this.attributes = description.attributes;
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
    case String:
      return 'S';
    case Number:
      return 'N';
    case Buffer:
      return 'B';
    default:
      throw(type + ' is not a matching type');
  }
}

module.exports = Schema;
