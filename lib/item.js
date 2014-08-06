function Item(json) {
  if (typeof json === 'object' && json !== null) {
    Object.keys(json).forEach(function(key) {
      this[key] = json[key];
    }.bind(this));
  }
}

Item.prototype.beforeSave = function(done) {
  done();
};

Item.prototype.save = function(done) {
  this.beforeSave(function(err) {
    if (err) {
      return done(err);
    }

    this.connection.putItem({
      TableName: this.schema.tableName(),
      Item: mapAttributes.call(this),
      Expected: this.schema.expected
    }, function(err, res) {
      done(err, this);
    }.bind(this));
  }.bind(this));
};

Item.prototype.update = function(done) {
  this.connection.updateItem({
    TableName: this.schema.tableName(),
    Key: mapKey.call(this),
    AttributeUpdates: mapAttributesUpdate.call(this)
  }, function(err, res) {
    done(err, this);
  }.bind(this));
};

Item.prototype.delete = function(done) {
  this.connection.deleteItem({
    TableName: this.schema.tableName(),
    Key: mapKey.call(this)
  }, function(err, res) {
    done(err, this);
  }.bind(this));
};

function mapAttributes() {
  return Object.keys(this.schema.attributes)
    .reduce(function(res, key) {
      if (this[key] !== null && this[key] !== undefined && this[key] !== '') {
        switch (this.schema.attributes[key]) {
          case String:
            res[key] = {S: this[key]};
            break;
          case Number:
            res[key] = {N: JSON.stringify(this[key])};
            break;
          case Buffer:
            res[key] = {B: this[key]};
            break;
        }
      }

      return res;
    }.bind(this), {});
}

function mapAttributesUpdate() {
  return Object.keys(this.schema.attributes)
    .reduce(function(res, key) {
      if (this.schema.key.indexOf(key) === -1) {
        if (this[key] !== null && this[key] !== undefined && this[key] !== '') {
          switch (this.schema.attributes[key]) {
            case String:
              res[key] = {Action: 'PUT', Value: {S: this[key]}};
              break;
            case Number:
              res[key] = {Action: 'PUT', Value: {N: JSON.stringify(this[key])}};
              break;
            case Buffer:
              res[key] = {Action: 'PUT', Value: {B: this[key]}};
              break;
          }
        } else {
          switch (this.schema.attributes[key]) {
            case String:
              res[key] = {Action: 'DELETE'};
              break;
            case Number:
              res[key] = {Action: 'DELETE'};
              break;
            case Buffer:
              res[key] = {Action: 'DELETE'};
              break;
          }
        }
      }

      return res;
    }.bind(this), {});
}

function mapKey() {
  return this.schema.key
    .reduce(function(res, key) {
      switch (this.schema.attributes[key]) {
        case String:
          res[key] = {S: this[key]};
          break;
        case Number:
          res[key] = {N: JSON.stringify(this[key])};
          break;
        case Buffer:
          res[key] = {B: this[key]};
          break;
      }

      return res;
    }.bind(this), {});
}

module.exports = Item;
