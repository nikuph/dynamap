var Schema = require('./schema');
var Item   = require('./item');

function Model() {
}

Model.compile = function(connection, schemaDescription) {
  function item() {
    Item.apply(this, arguments);
  }

  item.prototype = Object.create(Item.prototype);
  item.prototype.constructor = item;

  item.connection = item.prototype.connection = connection;
  item.schema = item.prototype.schema = new Schema(connection, schemaDescription);
  item.fetch = fetch.bind(item);
  item.query = query.bind(item);
  item.scan = scan.bind(item);
  item.all = all.bind(item);

  return item;
};

function fetch(key, done) {
  this.connection.getItem({
    TableName: this.schema.tableName(),
    Key: mapKey.call(this, key)
  }, function(err, res) {
    if (err) {
      return done(err);
    }

    done(null, parseItem.call(this, res.Item));
  }.bind(this));
}

function query(conditions, opts, done) {
  if (typeof done !== 'function') {
    done = opts;
    opts = {};
  }

  this.connection.query({
    TableName: this.schema.tableName(),
    IndexName: opts.index || null,
    KeyConditions: mapKeyConditions.call(this, conditions)
  }, function(err, res) {
    if (err) {
      return done(err);
    }

    done(null, parseItems.call(this, res.Items));
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
    TableName: this.schema.tableName(),
    ScanFilter: mapFilters.call(this, filters),
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

    done(null, parseItems.call(this, res.Items), res.LastEvaluatedKey);
  }.bind(this));
}

function all(opts, done) {
  if (typeof done !== 'function') {
    done = opts;
    opts = {};
  }

  this.scan([], opts, done);
}

function mapKey(key) {
  if (!Array.isArray(key)) {
    key = [key];
  }

  return key
    .reduce(function(res, val, idx) {
      var attribute = this.schema.key[idx];
      switch (this.schema.attributes[attribute]) {
        case String:
          res[attribute] = {S: val};
          break;
        case Number:
          res[attribute] = {N: JSON.stringify(val)};
          break;
        case Buffer:
          res[attribute] = {B: val};
          break;
      }

      return res;
    }.bind(this), {});
}

function mapKeyConditions(conditions) {
  return Object.keys(conditions)
    .reduce(function(res, key) {
      switch (this.schema.attributes[key]) {
        case String:
          res[key] = {AttributeValueList: [{S: conditions[key]}], ComparisonOperator: 'EQ'};
          break;
        case Number:
          res[key] = {AttributeValueList: [{N: JSON.stringify(conditions[key])}], ComparisonOperator: 'EQ'};
          break;
        case Buffer:
          res[key] = {AttributeValueList: [{B: conditions[key]}], ComparisonOperator: 'EQ'};
          break;
      }

      return res;
    }.bind(this), {});
}

function mapFilters(filters) {
  return filters
    .reduce(function(res, filter) {
      var parts = filter.split(' ');
      if (parts.length !== 3 && parts[1] !== 'NULL' && parts[1] !== 'NOT_NULL') {
        throw('Invalid filter definition \'' + filter + '\'');
      }

      res[parts[0]] = {
        ComparisonOperator: parts[1]
      };

      if (parts[2]) {
        switch (this.schema.attributes[parts[0]]) {
          case String:
            res[parts[0]].AttributeValueList = [{S: parts[2]}];
            break;
          case Number:
            res[parts[0]].AttributeValueList = [{N: parts[2]}];
            break;
          case Buffer:
            res[parts[0]].AttributeValueList = [{B: parts[2]}];
            break;
        }
      }

      return res;
    }.bind(this), {});
}

function parseItems(data) {
  return data.map(parseItem.bind(this));
}

function parseItem(data) {
  var instance = new this();

  Object.keys(data).forEach(function(key) {
    switch (this.schema.attributes[key]) {
      case String:
        instance[key] = data[key].S;
        break;
      case Number:
        instance[key] = parseInt(data[key].N, 10);
        break;
      case Buffer:
        instance[key] = data[key].B.toString();
        break;
    }
  }.bind(this));

  return instance;
}

module.exports = Model;
