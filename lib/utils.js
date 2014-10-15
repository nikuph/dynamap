var types = require('./types');

module.exports.toDynamoVal = function(type, val) {
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

  return res;
};

module.exports.fromDynamoVal = function(type, val) {
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

  return res;
};
