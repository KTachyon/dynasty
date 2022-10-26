(function() {
  var buildExclusiveStartKey, buildFilters, dataTrans, debug, extend, find, getKey, isObject, keys, map, mapKeys, scanFilterFunc;

  find = require('lodash/find');

  isObject = require('lodash/isObject');

  map = require('lodash/map');

  extend = require('lodash/extend');

  keys = require('lodash/keys');

  mapKeys = require('lodash/mapKeys');

  dataTrans = require('./data-translators');

  debug = require('debug')('dynasty:aws-translators');

  buildFilters = function(target, filters) {
    var filter, _i, _len, _results;
    if (filters) {
      _results = [];
      for (_i = 0, _len = filters.length; _i < _len; _i++) {
        filter = filters[_i];
        _results.push(scanFilterFunc(target, filter));
      }
      return _results;
    }
  };

  scanFilterFunc = function(target, filter) {
    target[filter.column] = {
      ComparisonOperator: filter.op || 'EQ',
      AttributeValueList: [{}]
    };
    target[filter.column].AttributeValueList[0][filter.type || 'S'] = filter.value;
    return target;
  };

  buildExclusiveStartKey = function(awsParams, params) {
    var awsValue, key, value, _ref;
    if (params.exclusiveStartKey) {
      awsValue = {};
      _ref = params.exclusiveStartKey;
      for (key in _ref) {
        value = _ref[key];
        awsValue[key] = dataTrans.toDynamo(params.exclusiveStartKey[key]);
      }
      return awsParams.ExclusiveStartKey = awsValue;
    }
  };

  module.exports.getKeySchema = function(tableDescription) {
    var getKeyAndType, hashKeyName, hashKeyType, rangeKeyName, rangeKeyType, _ref, _ref1;
    getKeyAndType = function(keyType) {
      var keyDataType, keyName, _ref, _ref1;
      keyName = (_ref = find(tableDescription.Table.KeySchema, function(key) {
        return key.KeyType === keyType;
      })) != null ? _ref.AttributeName : void 0;
      keyDataType = (_ref1 = find(tableDescription.Table.AttributeDefinitions, function(attribute) {
        return attribute.AttributeName === keyName;
      })) != null ? _ref1.AttributeType : void 0;
      return [keyName, keyDataType];
    };
    _ref = getKeyAndType('HASH'), hashKeyName = _ref[0], hashKeyType = _ref[1];
    _ref1 = getKeyAndType('RANGE'), rangeKeyName = _ref1[0], rangeKeyType = _ref1[1];
    return {
      hashKeyName: hashKeyName,
      hashKeyType: hashKeyType,
      rangeKeyName: rangeKeyName,
      rangeKeyType: rangeKeyType
    };
  };

  getKey = function(params, keySchema) {
    var key;
    if (!isObject(params)) {
      params = {
        hash: params + ''
      };
    }
    key = {};
    key[keySchema.hashKeyName] = {};
    key[keySchema.hashKeyName][keySchema.hashKeyType] = params.hash + '';
    if (params.range) {
      key[keySchema.rangeKeyName] = {};
      key[keySchema.rangeKeyName][keySchema.rangeKeyType] = params.range + '';
    }
    return key;
  };

  module.exports.deleteItem = function(params, options, keySchema) {
    var awsParams;
    awsParams = {
      TableName: this.name,
      Key: getKey(params, keySchema)
    };
    return this.parent.dynamo.deleteItem(awsParams).promise();
  };

  module.exports.batchGetItem = function(params, keySchema) {
    var awsParams, name;
    awsParams = {};
    awsParams.RequestItems = {};
    name = this.name;
    awsParams.RequestItems[this.name] = {
      Keys: map(params, function(param) {
        return getKey(param, keySchema);
      })
    };
    return this.parent.dynamo.batchGetItem(awsParams).promise().then(function(data) {
      return dataTrans.fromDynamo(data.Responses[name]);
    });
  };

  module.exports.getItem = function(params, options, keySchema) {
    var awsParams;
    awsParams = {
      TableName: this.name,
      Key: getKey(params, keySchema)
    };
    return this.parent.dynamo.getItem(awsParams).promise().then(function(data) {
      return dataTrans.fromDynamo(data.Item);
    });
  };

  module.exports.queryByHashKey = function(key, keySchema) {
    var awsParams, hashKeyName, hashKeyType;
    awsParams = {
      TableName: this.name,
      KeyConditions: {}
    };
    hashKeyName = keySchema.hashKeyName;
    hashKeyType = keySchema.hashKeyType;
    awsParams.KeyConditions[hashKeyName] = {
      ComparisonOperator: 'EQ',
      AttributeValueList: [{}]
    };
    awsParams.KeyConditions[hashKeyName].AttributeValueList[0][hashKeyType] = key;
    return this.parent.dynamo.query(awsParams).promise().then(function(data) {
      return dataTrans.fromDynamo(data.Items);
    });
  };

  module.exports.scan = function(params, options, keySchema) {
    var awsParams;
    if (params == null) {
      params = {};
    }
    awsParams = {
      TableName: this.name,
      ScanFilter: {},
      Limit: params.limit,
      TotalSegments: params.totalSegment,
      Segment: params.segment
    };
    if (params.attrsGet) {
      awsParams.AttributesToGet = params.attrsGet;
    }
    buildFilters(awsParams.ScanFilter, params.filters);
    return this.parent.dynamo.scan(awsParams).promise().then(function(data) {
      return dataTrans.fromDynamo(data.Items);
    });
  };

  module.exports.scanPaged = function(params, options, keySchema) {
    var awsParams;
    if (params == null) {
      params = {};
    }
    awsParams = {
      TableName: this.name,
      ScanFilter: {},
      Limit: params.limit,
      TotalSegments: params.totalSegment,
      Segment: params.segment
    };
    if (params.attrsGet) {
      awsParams.AttributesToGet = params.attrsGet;
    }
    buildExclusiveStartKey(awsParams, params);
    buildFilters(awsParams.ScanFilter, params.filters);
    return this.parent.dynamo.scan(awsParams).promise().then(function(data) {
      var lastEvaluatedKey, res;
      lastEvaluatedKey = dataTrans.fromDynamo(data.LastEvaluatedKey);
      res = {
        items: dataTrans.fromDynamo(data.Items),
        count: data.Count
      };
      if (lastEvaluatedKey) {
        res.lastEvaluatedKey = lastEvaluatedKey;
      }
      return res;
    });
  };

  module.exports.scanAll = function(params, options, keySchema) {
    var items, page, scanNext;
    items = [];
    page = 0;
    scanNext = (function(_this) {
      return function(exclusiveStartKey) {
        page++;
        if (exclusiveStartKey != null) {
          options.exclusiveStartKey = exclusiveStartKey;
        }
        return _this.scanPaged(params, options).then(function(res) {
          items = items.concat(res.items);
          if (res.lastEvaluatedKey) {
            return scanNext(exclusiveStartKey);
          } else {
            return items;
          }
        });
      };
    })(this);
    return scanNext();
  };

  module.exports.query = function(params, options, keySchema) {
    var awsParams;
    if (params == null) {
      params = {};
    }
    awsParams = {
      TableName: this.name,
      IndexName: params.indexName,
      KeyConditions: {},
      QueryFilter: {}
    };
    if (params.attrsGet) {
      awsParams.AttributesToGet = params.attrsGet;
    }
    buildFilters(awsParams.KeyConditions, params.keyConditions);
    buildFilters(awsParams.QueryFilter, params.filters);
    return this.parent.dynamo.query(awsParams).promise().then(function(data) {
      return dataTrans.fromDynamo(data.Items);
    });
  };

  module.exports.queryPaged = function(params, options, keySchema) {
    var awsParams;
    if (params == null) {
      params = {};
    }
    awsParams = {
      TableName: this.name,
      IndexName: params.indexName,
      KeyConditions: {},
      QueryFilter: {}
    };
    if (params.attrsGet) {
      awsParams.AttributesToGet = params.attrsGet;
    }
    buildExclusiveStartKey(awsParams, params);
    buildFilters(awsParams.KeyConditions, params.keyConditions);
    buildFilters(awsParams.QueryFilter, params.filters);
    return this.parent.dynamo.query(awsParams).promise().then(function(data) {
      var lastEvaluatedKey, res;
      lastEvaluatedKey = dataTrans.fromDynamo(data.LastEvaluatedKey);
      res = {
        items: dataTrans.fromDynamo(data.Items),
        count: data.Count
      };
      if (lastEvaluatedKey) {
        res.lastEvaluatedKey = lastEvaluatedKey;
      }
      return res;
    });
  };

  module.exports.putItem = function(obj, options) {
    var awsParams;
    awsParams = {
      TableName: this.name,
      Item: dataTrans.toDynamo(obj)
    };
    return this.parent.dynamo.putItem(awsParams).promise();
  };

  module.exports.updateItem = function(params, obj, options, keySchema) {
    var action, awsParams, calcUpdateExpression, expressionAttributeNames, expressionAttributeValues, i, key, updateExpression, _i, _len, _ref;
    key = getKey(params, keySchema);
    expressionAttributeValues = mapKeys(obj, function(value, key) {
      return ':' + key;
    });
    expressionAttributeValues = dataTrans.toDynamo(expressionAttributeValues);
    if (options != null ? options.expressionAttributeValues : void 0) {
      options.expressionAttributeValues = dataTrans.toDynamo(options.expressionAttributeValues);
      extend(expressionAttributeValues, options.expressionAttributeValues);
    }
    expressionAttributeNames = {};
    _ref = Object.keys(obj);
    for (i = _i = 0, _len = _ref.length; _i < _len; i = ++_i) {
      key = _ref[i];
      expressionAttributeNames["#" + key] = key;
    }
    action = (options != null ? options.incrementNumber : void 0) ? 'ADD' : 'SET';
    calcUpdateExpression = function(value, key) {
      if (options != null ? options.incrementNumber : void 0) {
        return "#" + key + " :" + key;
      } else {
        return "#" + key + " = :" + key;
      }
    };
    updateExpression = ("" + action + " ") + keys(mapKeys(obj, calcUpdateExpression)).join(',');
    awsParams = {
      TableName: this.name,
      Key: getKey(params, keySchema),
      ExpressionAttributeNames: expressionAttributeNames,
      ExpressionAttributeValues: expressionAttributeValues,
      UpdateExpression: updateExpression
    };
    if (options != null ? options.conditionExpression : void 0) {
      awsParams.ConditionExpression = options.conditionExpression;
    }
    return this.parent.dynamo.updateItem(awsParams).promise();
  };

}).call(this);
