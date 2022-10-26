(function() {
  var AWS, fromDynamo, isArray, isObject, nullUndefined, toDynamo;

  AWS = require('aws-sdk');

  isArray = require('lodash/isArray');

  isObject = require('lodash/isObject');

  fromDynamo = function(dbObj) {
    if (isArray(dbObj)) {
      return dbObj.map(function(item) {
        return AWS.DynamoDB.Converter.unmarshall(item);
      });
    } else if (isObject(dbObj)) {
      return AWS.DynamoDB.Converter.unmarshall(dbObj);
    } else {
      return dbObj;
    }
  };

  module.exports.fromDynamo = fromDynamo;

  nullUndefined = function(obj) {
    if (isArray(obj)) {
      return obj.map(function(value) {
        return nullUndefined(value);
      });
    } else if (isObject(obj)) {
      return Object.keys(obj).reduce(function(acc, key) {
        acc[key] = nullUndefined(obj[key]);
        return acc;
      }, {});
    } else if (obj === void 0) {
      return null;
    } else {
      return obj;
    }
  };

  toDynamo = function(item) {
    return AWS.DynamoDB.Converter.marshall(nullUndefined(item), {
      convertEmptyValues: true
    });
  };

  module.exports.toDynamo = toDynamo;

}).call(this);
