var utils = require('./utils');
var Item  = require('./item');

function Model() {
}

Model.compile = function(connection, schema) {
  function item() {
    Item.apply(this, arguments);
  }

  item.prototype = Object.create(Item.prototype);
  item.prototype.constructor = item;

  item.connection = item.prototype.connection = connection;
  item.schema = item.prototype.schema = schema;
  item.fetch = fetch.bind(item);
  item.afterFetch = afterFetch.bind(item);
  item.query = query.bind(item);
  item.afterQuery = afterQuery.bind(item);
  item.scan = scan.bind(item);
  item.afterScan = afterScan.bind(item);
  item.all = all.bind(item);
  item.afterAll = afterAll.bind(item);
  item.copyItems = copyItems.bind(item);

  return item;
};

function afterFetch(item, done) {
  done(null, item);
}

function fetch(key, done) {
  this.connection.getItem({
    TableName: this.schema.tableName(),
    Key: mapKey.call(this, key)
  }, function(err, res) {
    if (err) {
      return done(err);
    }

    var item = parseItem.call(this, res.Item);
    this.afterFetch(item, done);
  }.bind(this));
}

function afterQuery(items, lastEvaluatedKey, done) {
  done(null, items, lastEvaluatedKey);
}

function query(conditions, opts, done) {
  if (typeof done !== 'function') {
    done = opts;
    opts = {};
  }

  var params = {
    TableName: this.schema.tableName(),
    IndexName: opts.index || null,
    KeyConditions: mapConditions.call(this, conditions)
  };

  this.connection.query(params, function(err, res) {
    if (err) {
      return done(err);
    }

    var items = parseItems.call(this, res.Items);
    this.afterQuery(items, res.LastEvaluatedKey, done);
  }.bind(this));
}

function _scan(filters, opts, done) {
  if (!Array.isArray(filters)) {
    filters = [filters];
  }

  var params = {
    TableName: this.schema.tableName(),
    ScanFilter: mapConditions.call(this, filters),
    Limit: opts.limit || null,
    ExclusiveStartKey: opts.startKey || null
  };

  if (opts.attributes) {
    params.AttributesToGet = opts.attributes;
  }

  this.connection.scan(params, function(err, res) {
    if (err) {
      return done(err);
    }

    var items = parseItems.call(this, res.Items);
    done(null, items, res.LastEvaluatedKey);
  }.bind(this));
}

function afterScan(items, lastEvaluatedKey, done) {
  done(null, items, lastEvaluatedKey);
}

function scan(filters, opts, done) {
  if (typeof done !== 'function') {
    done = opts;
    opts = {};
  }

  _scan.call(this, filters, opts, function(err, items, lastEvaluatedKey) {
    if (err) {
      return done(err);
    }

    this.afterScan(items, lastEvaluatedKey, done);
  }.bind(this));
}


function afterAll(items, lastEvaluatedKey, done) {
  done(null, items, lastEvaluatedKey);
}

function all(opts, done) {
  if (typeof done !== 'function') {
    done = opts;
    opts = {};
  }

  _scan.call(this, [], opts, function(err, items, lastEvaluatedKey) {
    if (err) {
      return done(err);
    }

    this.afterAll(items, lastEvaluatedKey, done);
  }.bind(this));
}

function mapKey(key) {
  if (!Array.isArray(key)) {
    key = [key];
  }

  return key
    .reduce(function(res, val, idx) {
      var attribute = this.schema.key[idx];
      res[attribute] = utils.toDynamoVal(this.schema.attributes[attribute], val);

      return res;
    }.bind(this), {});
}

function mapConditions(conditions) {
  if (!Array.isArray(conditions)) {
    conditions = [conditions];
  }

  return conditions
    .reduce(function(res, condition) {
      res[condition.attribute] = {
        AttributeValueList: [utils.toDynamoVal(this.schema.attributes[condition.attribute], condition.value)],
        ComparisonOperator: condition.operator || 'EQ'
      };

      return res;
    }.bind(this), {});
}

function parseItems(data) {
  return data.map(parseItem.bind(this));
}

function parseItem(data) {
  if (!data) {
    return null;
  }

  var instance = new this();

  Object.keys(data).forEach(function(key) {
    instance[key] = utils.fromDynamoVal(this.schema.attributes[key], data[key]);
  }.bind(this));

  return instance;
}

function copyItems(destinationTable, opts, done) {
  if (typeof done !== 'function') {
    done = opts;
    opts = {};
  }

  opts.limit = opts.limit || 10;

  this.all(opts, function(err, items, lastEvaluatedKey) {
    var error;

    if (err) {
      done(err);
    }

    var itemsRemaining = items.length;
    items.forEach(function(item) {
      item.save({tableName: destinationTable}, function(err, res) {
        if (err) {
          error = err;
        }

        --itemsRemaining;
        if (0 === itemsRemaining) {
          if (error || !lastEvaluatedKey) {
            return done(error);
          }

          opts.startKey = lastEvaluatedKey;
          this.copyItems(destinationTable, opts, done);
        }
      }.bind(this));
    }.bind(this));
  }.bind(this));
}

module.exports = Model;
