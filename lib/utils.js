var types = require('./types');

function toDynamoVal(type, val) {
  var res;

  switch (type) {
    case Number:
      res = {N: JSON.stringify(val)};
      break;
    case String:
      res = {S: val};
      break;
    case Buffer:
      res = {B: val};
      break;
    case Boolean:
      res = {BOOL: val};
      break;
    case types.StringSet:
      res = {SS: val};
      break;
    case types.NumberSet:
      res = {NS: val};
      break;
    case types.BufferSet:
      res = {BS: val};
      break;
  }

  if (res) {
    return res;
  }

  if (type instanceof types.List) {
    res = {
      L: val.map(function(v) {
        return toDynamoVal(type.type, v);
      })
    };
  } else if (type instanceof types.Map) {
    res = {
      M: Object.keys(val).reduce(function(res, key) {
        if (val[key] !== null && val[key] !== undefined && val[key] !== '' && (!Array.isArray(val[key]) || val[key].length)) {
          res[key] = toDynamoVal(type.attributes[key], val[key]);
        }

        return res;
      }, {})
    };
  }

  return res;
}

function fromDynamoVal(type, val) {
  var res;

  switch (type) {
    case Number:
      res = Number(val.N);
      break;
    case String:
      res = val.S;
      break;
    case Buffer:
      res = val.B;
      break;
    case Boolean:
      res = val.BOOL;
      break;
    case types.StringSet:
      res = val.SS;
      break;
    case types.NumberSet:
      res = val.NS;
      break;
    case types.BufferSet:
      res = val.BS;
      break;
  }

  if (res) {
    return res;
  }

  if (type instanceof types.List) {
    res = val.L.map(function(v) {
      return fromDynamoVal(type.type, v);
    });
  } else if (type instanceof types.Map) {
    res = Object.keys(val.M).reduce(function(res, key) {
      res[key] = fromDynamoVal(type.attributes[key], val.M[key]);

      return res;
    }, {});
  }

  return res;
}

module.exports.toDynamoVal   = toDynamoVal;
module.exports.fromDynamoVal = fromDynamoVal;
