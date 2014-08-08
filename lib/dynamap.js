var AWS    = require('aws-sdk');
var Model  = require('./model');
var Schema = require('./schema');

var _dynamodb = null;
var _opts     = {};

module.exports.initialize = function(opts) {
  if (!opts || !opts.accessKeyId || !opts.secretAccessKey || !opts.region) {
    throw('missing accessKeyId or secretAccessKey or region!');
  }

  _dynamodb = new AWS.DynamoDB({
    apiVersion: '2012-08-10',
    accessKeyId: opts.accessKeyId,
    secretAccessKey: opts.secretAccessKey,
    region: opts.region,
    logger: opts.logger || process.stdout
  });

  _opts = opts;

  return this;
};

module.exports.model = function(schemaDescription) {
  if (!_dynamodb) {
    throw('dynamap isn\'t initialized');
  }

  var schema = new Schema(_dynamodb, schemaDescription, _opts);

  return Model.compile(_dynamodb, schema);
};
