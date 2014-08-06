var AWS   = require('aws-sdk');
var Model = require('./model');

var dynamodb;

module.exports.initialize = function(opts) {
  if (!opts || !opts.accessKeyId || !opts.secretAccessKey) {
    throw('missing accessKeyId or secretAccessKey!');
  }

  dynamodb = new AWS.DynamoDB({
    apiVersion: '2012-08-10',
    region: opts.region || 'eu-west-1',
    logger: opts.logger || process.stdout,
    accessKeyId: opts.accessKeyId,
    secretAccessKey: opts.secretAccessKey
  });

  return this;
};

module.exports.Model = function(schemaDescription) {
  if (!dynamodb) {
    throw('dynamap isn\'t initialized');
  }

  return Model.compile(dynamodb, schemaDescription);
};
