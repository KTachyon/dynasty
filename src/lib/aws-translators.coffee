_ = require('lodash')
dataTrans = require('./data-translators')
debug = require('debug')('dynasty:aws-translators')

buildFilters = (target, filters) ->
  if filters
    scanFilterFunc(target, filter) for filter in filters

scanFilterFunc = (target, filter) ->
  target[filter.column] =
    ComparisonOperator: filter.op || 'EQ'
    AttributeValueList: [{}]
  target[filter.column].AttributeValueList[0][filter.type || 'S'] = filter.value
  target

buildExclusiveStartKey = (awsParams, params) ->
  if params.exclusiveStartKey
    awsValue = {}
    for key, value of params.exclusiveStartKey
      awsValue[key] = dataTrans.toDynamo(params.exclusiveStartKey[key])
    awsParams.ExclusiveStartKey = awsValue

module.exports.getKeySchema = (tableDescription) ->
  getKeyAndType = (keyType) ->
    keyName = _.find tableDescription.Table.KeySchema, (key) ->
      key.KeyType is keyType
    ?.AttributeName

    keyDataType = _.find tableDescription.Table.AttributeDefinitions,
    (attribute) ->
      attribute.AttributeName is keyName
    ?.AttributeType
    [keyName, keyDataType]

  [hashKeyName, hashKeyType] = getKeyAndType 'HASH'
  [rangeKeyName, rangeKeyType] = getKeyAndType 'RANGE'

  hashKeyName: hashKeyName
  hashKeyType: hashKeyType
  rangeKeyName: rangeKeyName
  rangeKeyType: rangeKeyType

getKey = (params, keySchema) ->
  if !_.isObject params
    params = hash: params+''

  key = {}
  key[keySchema.hashKeyName] = {}
  key[keySchema.hashKeyName][keySchema.hashKeyType] = params.hash+''

  if params.range
    key[keySchema.rangeKeyName] = {}
    key[keySchema.rangeKeyName][keySchema.rangeKeyType] = params.range+''

  key

module.exports.deleteItem = (params, options, keySchema) ->
  awsParams =
    TableName: @name
    Key: getKey(params, keySchema)
  @parent.dynamo.deleteItem(awsParams).promise()

module.exports.batchGetItem = (params, keySchema) ->
  awsParams = {}
  awsParams.RequestItems = {}
  name = @name
  awsParams.RequestItems[@name] = Keys: _.map(params, (param) -> getKey(param, keySchema))
  @parent.dynamo.batchGetItem(awsParams).promise()
    .then (data) ->
      dataTrans.fromDynamo(data.Responses[name])

module.exports.getItem = (params, options, keySchema) ->
  awsParams =
    TableName: @name
    Key: getKey(params, keySchema)

  @parent.dynamo.getItem(awsParams).promise()
    .then (data)->
      dataTrans.fromDynamo(data.Item)

module.exports.queryByHashKey = (key, keySchema) ->
  awsParams =
    TableName: @name
    KeyConditions: {}

  hashKeyName = keySchema.hashKeyName
  hashKeyType = keySchema.hashKeyType

  awsParams.KeyConditions[hashKeyName] =
    ComparisonOperator: 'EQ'
    AttributeValueList: [{}]
  awsParams.KeyConditions[hashKeyName].AttributeValueList[0][hashKeyType] = key

  @parent.dynamo.query(awsParams).promise()
    .then (data) ->
      dataTrans.fromDynamo(data.Items)

module.exports.scan = (params, options, keySchema) ->
  params ?= {}
  awsParams =
    TableName: @name
    ScanFilter: {}
    Limit: params.limit
    TotalSegments: params.totalSegment
    Segment: params.segment

  awsParams.AttributesToGet = params.attrsGet if params.attrsGet

  buildFilters(awsParams.ScanFilter, params.filters)

  @parent.dynamo.scan(awsParams).promise()
    .then (data)->
      dataTrans.fromDynamo(data.Items)

module.exports.scanPaged = (params, options, keySchema) ->
  params ?= {}
  awsParams =
    TableName: @name
    ScanFilter: {}
    Limit: params.limit
    TotalSegments: params.totalSegment
    Segment: params.segment

  awsParams.AttributesToGet = params.attrsGet if params.attrsGet


  buildExclusiveStartKey(awsParams, params)
  buildFilters(awsParams.ScanFilter, params.filters)

  @parent.dynamo.scan(awsParams).promise()
    .then (data)->
      lastEvaluatedKey = dataTrans.fromDynamo(data.LastEvaluatedKey)
      res =
        items: dataTrans.fromDynamo(data.Items)
        count: data.Count
      if lastEvaluatedKey
        res.lastEvaluatedKey = lastEvaluatedKey
      res

module.exports.scanAll = (params, options, keySchema) ->
  items = []
  page = 0
  scanNext = (exclusiveStartKey) =>
    page++
    if exclusiveStartKey?
      options.exclusiveStartKey = exclusiveStartKey
    #console.log("Scanning page #{page} (#{items.length} fetched so far)")
    @scanPaged(params, options)
      .then (res) ->
        items = items.concat(res.items)
        if res.lastEvaluatedKey
          scanNext(exclusiveStartKey)
        else
          items
  scanNext()

module.exports.query = (params, options, keySchema) ->
  params ?= {}
  awsParams =
    TableName: @name
    IndexName: params.indexName
    KeyConditions: {}
    QueryFilter: {}
  awsParams.AttributesToGet = params.attrsGet if params.attrsGet

  buildFilters(awsParams.KeyConditions, params.keyConditions)
  buildFilters(awsParams.QueryFilter, params.filters)

  @parent.dynamo.query(awsParams).promise()
    .then (data) ->
      dataTrans.fromDynamo(data.Items)

module.exports.queryPaged = (params, options, keySchema) ->
  params ?= {}
  awsParams =
    TableName: @name
    IndexName: params.indexName
    KeyConditions: {}
    QueryFilter: {}
  awsParams.AttributesToGet = params.attrsGet if params.attrsGet

  buildExclusiveStartKey(awsParams, params)
  buildFilters(awsParams.KeyConditions, params.keyConditions)
  buildFilters(awsParams.QueryFilter, params.filters)

  @parent.dynamo.query(awsParams).promise()
    .then (data)->
      lastEvaluatedKey = dataTrans.fromDynamo(data.LastEvaluatedKey)
      res =
        items: dataTrans.fromDynamo(data.Items)
        count: data.Count
      if lastEvaluatedKey
        res.lastEvaluatedKey = lastEvaluatedKey
      res

module.exports.putItem = (obj, options) ->
  awsParams =
    TableName: @name
    Item: dataTrans.toDynamo(obj)

  @parent.dynamo.putItem(awsParams).promise()

module.exports.updateItem = (params, obj, options, keySchema) ->
  key = getKey(params, keySchema)

  # Set up the Expression Attribute Values map.
  expressionAttributeValues = _.mapKeys obj, (value, key) -> return ':' + key
  expressionAttributeValues = dataTrans.toDynamo(expressionAttributeValues)
  # Allow setting arbitrary attribute values
  if options?.expressionAttributeValues
    options.expressionAttributeValues = dataTrans.toDynamo(options.expressionAttributeValues)
    _.extend(expressionAttributeValues, options.expressionAttributeValues)

  # Setup ExpressionAttributeNames mapping key -> #key so we don't bump into
  # reserved words
  expressionAttributeNames = {}
  expressionAttributeNames["##{key}"] = key for key, i in Object.keys(obj)

  # Set up the Update Expression
  action = if options?.incrementNumber then 'ADD' else 'SET'
  calcUpdateExpression = (value, key) ->
    if options?.incrementNumber
      "##{key} :#{key}"
    else
      "##{key} = :#{key}"
  updateExpression = "#{action} " + _.keys(_.mapKeys obj, calcUpdateExpression).join ','

  awsParams =
    TableName: @name
    Key: getKey(params, keySchema)
    ExpressionAttributeNames: expressionAttributeNames
    ExpressionAttributeValues: expressionAttributeValues
    UpdateExpression: updateExpression

  if options?.conditionExpression
    awsParams.ConditionExpression = options.conditionExpression
  @parent.dynamo.updateItem(awsParams).promise()
