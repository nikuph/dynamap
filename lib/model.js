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

  item.connection      = item.prototype.connection = connection;
  item.schema          = item.prototype.schema = schema;
  item.afterParseItem  = afterParseItem.bind(item);
  item.afterParseItems = afterParseItems.bind(item);
  item.get             = get.bind(item);
  item.batchGet        = batchGet.bind(item);
  item.query           = query.bind(item);
  item.scan            = scan.bind(item);
  item.search          = search.bind(item);

  return item;
};

function get(key, done) {
  var params = {
    TableName : this.schema.tableName(),
    Key       : mapKey.call(this, key)
  };

  this.connection.getItem(params, function(err, res) {
    if (err) {
      return done(err);
    }

    if (!res.Item) {
      return done(null, null);
    }

    var item = parseItem.call(this, res.Item);
    this.afterParseItem(item, function(err, item) {
      if (err) {
        return done(err);
      }

      done(null, item);
    });
  }.bind(this));
}

function batchGet(keys, done) {
  var params = {
    RequestItems: {}
  };

  params.RequestItems[this.schema.tableName()] = {
    Keys: keys.map(function(key) { return mapKey.call(this, key); }.bind(this))
  };

  this.connection.batchGetItem(params, function(err, res) {
    if (err) {
      return done(err);
    }

    var items = parseItems.call(this, res.Responses[this.schema.tableName()]);
    this.afterParseItems(items, function(err, items) {
      if (err) {
        return done(err);
      }

      done(null, items, res.UnprocessedKeys);
    });

  }.bind(this));
}

function query(conditions, opts, done) {
  if (typeof done !== 'function') {
    done = opts;
    opts = {};
  }

  var params = {
    TableName     : this.schema.tableName(),
    IndexName     : opts.index || null,
    KeyConditions : mapConditions.call(this, conditions)
  };

  this.connection.query(params, function(err, res) {
    if (err) {
      return done(err);
    }

    var items = parseItems.call(this, res.Items);
    this.afterParseItems(items, function(err, items) {
      if (err) {
        return done(err);
      }

      done(null, items, res.LastEvaluatedKey);
    });
  }.bind(this));
}

function scan(filters, opts, done) {
  if (typeof done !== 'function') {
    done = opts;
    opts = {};
  }

  if (!Array.isArray(filters)) {
    filters = [filters];
  }

  var params = {
    TableName         : this.schema.tableName(),
    ScanFilter        : mapConditions.call(this, filters),
    Limit             : opts.limit || null,
    ExclusiveStartKey : opts.startKey || null
  };

  if (opts.attributes) {
    params.AttributesToGet = opts.attributes;
  }

  if (opts.conditionalOperator) {
    params.ConditionalOperator = opts.conditionalOperator;
  }

  this.connection.scan(params, function(err, res) {
    if (err) {
      return done(err);
    }

    var items = parseItems.call(this, res.Items);
    this.afterParseItems(items, function(err, items) {
      if (err) {
        return done(err);
      }

      done(null, items, res.LastEvaluatedKey);
    });
  }.bind(this));
}

function search(queryString, opts, done) {
  if (typeof done !== 'function') {
    done = opts;
    opts = {};
  }

  var filters;
  if (queryString) {
    filters = this.schema.searchable.map(function(attr) {
      return {
        attribute : attr,
        value     : queryString,
        operator  : 'CONTAINS'
      };
    });

    opts.conditionalOperator = 'OR';
  } else {
    filters = [];
  }

  scan.call(this, filters, opts, done);
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

function afterParseItem(item, done) {
  return done(null, item);
}

function parseItems(data) {
  return data.map(parseItem.bind(this));
}

function afterParseItems(item, done) {
  return done(null, item);
}

module.exports = Model;
