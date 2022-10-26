(function() {
  var Table, awsTrans, compact, dataTrans, debug, isFunction, size, values;

  awsTrans = require('./aws-translators');

  dataTrans = require('./data-translators');

  size = require('lodash/size');

  compact = require('lodash/compact');

  values = require('lodash/values');

  isFunction = require('lodash/isFunction');

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

    Table.prototype.batchFind = function(params, callback) {
      if (callback == null) {
        callback = null;
      }
      debug("batchFind() - " + params);
      return this.key.then(awsTrans.batchGetItem.bind(this, params, callback));
    };

    Table.prototype.findAll = function(params, callback) {
      if (callback == null) {
        callback = null;
      }
      debug("findAll() - " + params);
      return this.key.then(awsTrans.queryByHashKey.bind(this, params, callback));
    };

    Table.prototype.find = function(params, options, callback) {
      if (options == null) {
        options = {};
      }
      if (callback == null) {
        callback = null;
      }
      debug("find() - " + params);
      return this.key.then(awsTrans.getItem.bind(this, params, options, callback));
    };

    Table.prototype.scan = function(params, options, callback) {
      if (options == null) {
        options = {};
      }
      if (callback == null) {
        callback = null;
      }
      debug("scan() - " + params);
      return this.key.then(awsTrans.scan.bind(this, params, options, callback));
    };

    Table.prototype.scanPaged = function(params, options, callback) {
      if (options == null) {
        options = {};
      }
      if (callback == null) {
        callback = null;
      }
      debug("scanPaged() - " + params);
      return this.key.then(awsTrans.scanPaged.bind(this, params, options, callback));
    };

    Table.prototype.scanAll = function(params, options, callback) {
      if (options == null) {
        options = {};
      }
      if (callback == null) {
        callback = null;
      }
      debug("scanAll() - " + params);
      return this.key.then(awsTrans.scanAll.bind(this, params, options, callback));
    };

    Table.prototype.query = function(params, options, callback) {
      if (options == null) {
        options = {};
      }
      if (callback == null) {
        callback = null;
      }
      debug("query() - " + params);
      return this.key.then(awsTrans.query.bind(this, params, options, callback));
    };

    Table.prototype.queryPaged = function(params, options, callback) {
      if (options == null) {
        options = {};
      }
      if (callback == null) {
        callback = null;
      }
      debug("query() - " + params);
      return this.key.then(awsTrans.queryPaged.bind(this, params, options, callback));
    };

    Table.prototype.insert = function(obj, options, callback) {
      if (options == null) {
        options = {};
      }
      if (callback == null) {
        callback = null;
      }
      debug("insert() - " + JSON.stringify(obj));
      if (isFunction(options)) {
        callback = options;
        options = {};
      }
      return this.key.then(awsTrans.putItem.bind(this, obj, options, callback));
    };

    Table.prototype.remove = function(params, options, callback) {
      if (callback == null) {
        callback = null;
      }
      return this.key.then(awsTrans.deleteItem.bind(this, params, options, callback));
    };

    Table.prototype.update = function(params, obj, options, callback) {
      if (callback == null) {
        callback = null;
      }
      debug("update() - " + JSON.stringify(obj));
      if (isFunction(options)) {
        callback = options;
        options = {};
      }
      return this.key.then(awsTrans.updateItem.bind(this, params, obj, options, callback));
    };


    /*
    Table Operations
     */

    Table.prototype.describe = function(callback) {
      if (callback == null) {
        callback = null;
      }
      debug('describe() - ' + this.name);
      return this.parent.dynamo.describeTable({
        TableName: this.name
      }).promise();
    };

    Table.prototype.drop = function(callback) {
      if (callback == null) {
        callback = null;
      }
      return this.parent.drop(this.name, callback);
    };

    return Table;

  })();

  module.exports = Table;

}).call(this);
