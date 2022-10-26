(function() {
  var Table, awsTrans, compact, dataTrans, debug, size, values;

  awsTrans = require('./aws-translators');

  dataTrans = require('./data-translators');

  size = require('lodash/size');

  compact = require('lodash/compact');

  values = require('lodash/values');

  debug = require('debug')('dynasty');

  Table = (function() {
    function Table(parent, name) {
      this.parent = parent;
      this.name = name;
      this.key = this.describe().then(awsTrans.getKeySchema).then((function(_this) {
        return function(keySchema) {
          _this.hasRangeKey = 4 === size(compact(values(keySchema)));
          return keySchema;
        };
      })(this));
    }


    /*
    Item Operations
     */

    Table.prototype.batchFind = function(params) {
      debug("batchFind() - " + params);
      return this.key.then(awsTrans.batchGetItem.bind(this, params));
    };

    Table.prototype.findAll = function(params) {
      debug("findAll() - " + params);
      return this.key.then(awsTrans.queryByHashKey.bind(this, params));
    };

    Table.prototype.find = function(params, options) {
      if (options == null) {
        options = {};
      }
      debug("find() - " + params);
      return this.key.then(awsTrans.getItem.bind(this, params, options));
    };

    Table.prototype.scan = function(params, options) {
      if (options == null) {
        options = {};
      }
      debug("scan() - " + params);
      return this.key.then(awsTrans.scan.bind(this, params, options));
    };

    Table.prototype.scanPaged = function(params, options) {
      if (options == null) {
        options = {};
      }
      debug("scanPaged() - " + params);
      return this.key.then(awsTrans.scanPaged.bind(this, params, options));
    };

    Table.prototype.scanAll = function(params, options) {
      if (options == null) {
        options = {};
      }
      debug("scanAll() - " + params);
      return this.key.then(awsTrans.scanAll.bind(this, params, options));
    };

    Table.prototype.query = function(params, options) {
      if (options == null) {
        options = {};
      }
      debug("query() - " + params);
      return this.key.then(awsTrans.query.bind(this, params, options));
    };

    Table.prototype.queryPaged = function(params, options) {
      if (options == null) {
        options = {};
      }
      debug("query() - " + params);
      return this.key.then(awsTrans.queryPaged.bind(this, params, options));
    };

    Table.prototype.insert = function(obj, options) {
      if (options == null) {
        options = {};
      }
      debug("insert() - " + JSON.stringify(obj));
      return this.key.then(awsTrans.putItem.bind(this, obj, options));
    };

    Table.prototype.remove = function(params, options) {
      if (options == null) {
        options = {};
      }
      return this.key.then(awsTrans.deleteItem.bind(this, params, options));
    };

    Table.prototype.update = function(params, obj, options) {
      if (options == null) {
        options = {};
      }
      debug("update() - " + JSON.stringify(obj));
      return this.key.then(awsTrans.updateItem.bind(this, params, obj, options));
    };


    /*
    Table Operations
     */

    Table.prototype.describe = function() {
      debug('describe() - ' + this.name);
      return this.parent.dynamo.describeTable({
        TableName: this.name
      }).promise();
    };

    Table.prototype.drop = function() {
      return this.parent.drop(this.name);
    };

    return Table;

  })();

  module.exports = Table;

}).call(this);
