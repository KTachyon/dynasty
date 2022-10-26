(function() {
  var Dynasty, Table, aws, debug, isObject, isString;

  aws = require('aws-sdk');

  isObject = require('lodash/isObject');

  isString = require('lodash/isString');

  debug = require('debug')('dynasty');

  Table = require('./lib').Table;

  Dynasty = (function() {
    function Dynasty(credentials, url) {
      debug("dynasty constructed.");
      credentials.region = credentials.region || 'us-east-1';
      credentials.apiVersion = '2012-08-10';
      if (url && isString(url)) {
        debug("connecting to local dynamo at " + url);
        credentials.endpoint = new aws.Endpoint(url);
      }
      this.dynamo = new aws.DynamoDB(credentials);
      this.name = 'Dynasty';
      this.tables = {};
    }

    Dynasty.prototype.table = function(name) {
      return this.tables[name] = this.tables[name] || new Table(this, name);
    };


    /*
    Table Operations
     */

    Dynasty.prototype.describe = function(name) {
      debug("describe() - " + name);
      return this.dynamo.describeTable({
        TableName: name
      }).promise();
    };

    Dynasty.prototype.convertFromDynamo = function(dynamoData) {
      return lib['data-translators'].fromDynamo(dynamoData);
    };

    Dynasty.prototype.convertToDynamo = function(data) {
      return lib['data-translators'].toDynamo(data);
    };

    return Dynasty;

  })();

  module.exports = function(credentials, url) {
    return new Dynasty(credentials, url);
  };

}).call(this);
