var utils = require('./utils');

function Item(json) {
  if (typeof json === 'object' && json !== null) {
    Object.keys(json).forEach(function(key) {
      this[key] = json[key];
    }.bind(this));
  }

  this.initialize();
}

Item.prototype.initialize = function(done) {
};

Item.prototype.beforeSave = function(done) {
  done();
};

Item.prototype.save = function(opts, done) {
  if (typeof done !== 'function') {
    done = opts;
    opts = {};
  }

  this.beforeSave(function(err) {
    if (err) {
      return done(err);
    }

    var params = {
      TableName: opts.tableName || this.schema.tableName(),
      Item: mapAttributes.call(this),
      Expected: this.schema.expected
    };

    this.connection.putItem(params, function(err, res) {
      done(err, this);
    }.bind(this));
  }.bind(this));
};

Item.prototype.beforeUpdate = function(done) {
  done();
};

Item.prototype.update = function(done) {
  this.beforeUpdate(function(err) {
    if (err) {
      return done(err);
    }

    var params = {
      TableName: this.schema.tableName(),
      Key: mapKey.call(this),
      AttributeUpdates: mapAttributesUpdate.call(this)
    };

    this.connection.updateItem(params, function(err, res) {
      done(err, this);
    }.bind(this));
  }.bind(this));
};

Item.prototype.beforeDelete = function(done) {
  done();
};

Item.prototype.delete = function(done) {
  this.beforeDelete(function(err) {
    if (err) {
      return done(err);
    }

    this.connection.deleteItem({
      TableName: this.schema.tableName(),
      Key: mapKey.call(this)
    }, function(err, res) {
      done(err, this);
    }.bind(this));
  }.bind(this));
};

function mapAttributes() {
  return Object.keys(this.schema.attributes)
    .reduce(function(res, key) {
      if (this[key] !== null && this[key] !== undefined && this[key] !== '') {
        res[key] = utils.toDynamoVal(this.schema.attributes[key], this[key]);
      }

      return res;
    }.bind(this), {});
}

function mapAttributesUpdate() {
  return Object.keys(this.schema.attributes)
    .reduce(function(res, key) {
      if (this.schema.key.indexOf(key) === -1) {
        if (this[key] !== null && this[key] !== undefined && this[key] !== '') {
          res[key] = {
            Action: 'PUT',
            Value: utils.toDynamoVal(this.schema.attributes[key], this[key])
          };
        } else {
          res[key] = {Action: 'DELETE'};
        }
      }

      return res;
    }.bind(this), {});
}

function mapKey() {
  return this.schema.key
    .reduce(function(res, key) {
      res[key] = utils.toDynamoVal(this.schema.attributes[key], this[key]);
      return res;
    }.bind(this), {});
}

module.exports = Item;
