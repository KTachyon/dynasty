(function() {
  var Dynasty, Table, aws, debug, isObject, isString,
    __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; };

  aws = require('aws-sdk');

  isObject = require('lodash/isObject');

  isString = require('lodash/isString');

  debug = require('debug')('dynasty');

  Table = require('./lib').Table;

  Dynasty = (function() {
    function Dynasty(credentials, url) {
      this.loadAllTables = __bind(this.loadAllTables, this);
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

    Dynasty.prototype.loadAllTables = function() {
      return this.list().then((function(_this) {
        return function(data) {
          var tableName, _i, _len, _ref;
          _ref = data.TableNames;
          for (_i = 0, _len = _ref.length; _i < _len; _i++) {
            tableName = _ref[_i];
            _this.table(tableName);
          }
          return _this.tables;
        };
      })(this));
    };

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

    Dynasty.prototype.list = function(params) {
      var awsParams;
      debug("list() - " + params);
      awsParams = {};
      if (params === !null) {
        if (isString(params)) {
          awsParams.ExclusiveStartTableName = params;
        } else if (isObject(params)) {
          if (params.limit === !null) {
            awsParams.Limit = params.limit;
          } else if (params.start === !null) {
            awsParams.ExclusiveStartTableName = params.start;
          }
        }
      }
      return this.dynamo.listTables(awsParams).promise();
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
