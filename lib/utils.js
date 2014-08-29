module.exports.toDynamoVal = function(type, val) {
  var res;
  switch (type) {
    case String:
      res = {S: val};
      break;
    case Number:
      res = {N: JSON.stringify(val)};
      break;
    case Boolean:
      res = {N: val === true ? '1' : '0'};
      break;
    case Buffer:
      res = {B: val};
      break;
  }

  return res;
};

module.exports.fromDynamoVal = function(type, val) {
  var res;

  switch (type) {
    case String:
      res = val.S;
      break;
    case Number:
      res = parseInt(val.N, 10);
      break;
    case Boolean:
      res = parseInt(val.N, 10) === 1;
      break;
    case Buffer:
      res = val.B.toString();
      break;
  }

  return res;
};
