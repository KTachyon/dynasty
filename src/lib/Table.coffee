awsTrans = require('./aws-translators')
dataTrans = require('./data-translators')
size = require('lodash/size')
compact = require('lodash/compact')
values = require('lodash/values')
debug = require('debug')('dynasty')

class Table

  constructor: (@parent, @name) ->
    @key = @describe().then(awsTrans.getKeySchema).then (keySchema)=>
      @hasRangeKey = (4 == size compact values keySchema)
      keySchema

  ###
  Item Operations
  ###

  # Wrapper around DynamoDB's batchGetItem
  batchFind: (params) ->
    debug "batchFind() - #{params}"
    @key.then awsTrans.batchGetItem.bind(this, params)

  findAll: (params) ->
    debug "findAll() - #{params}"
    @key.then awsTrans.queryByHashKey.bind(this, params)

  # Wrapper around DynamoDB's getItem
  find: (params, options = {}) ->
    debug "find() - #{params}"
    @key.then awsTrans.getItem.bind(this, params, options)

  # Wrapper around DynamoDB's scan
  scan: (params, options = {}) ->
    debug "scan() - #{params}"
    @key.then awsTrans.scan.bind(this, params, options)

  scanPaged: (params, options = {}) ->
    debug "scanPaged() - #{params}"
    @key.then awsTrans.scanPaged.bind(this, params, options)

  # Will call scan, page through each page and return all results
  scanAll: (params, options = {}) ->
    debug "scanAll() - #{params}"
    @key.then awsTrans.scanAll.bind(this, params, options)

  # Wrapper around DynamoDB's query
  query: (params, options = {}) ->
    debug "query() - #{params}"
    @key.then awsTrans.query.bind(this, params, options)

  # Wrapper around DynamoDB's query
  queryPaged: (params, options = {}) ->
    debug "query() - #{params}"
    @key.then awsTrans.queryPaged.bind(this, params, options)


  # Wrapper around DynamoDB's putItem
  insert: (obj, options = {}) ->
    debug "insert() - " + JSON.stringify obj
    @key.then awsTrans.putItem.bind(this, obj, options)

  remove: (params, options = {}) ->
    @key.then awsTrans.deleteItem.bind(this, params, options)

  # Wrapper around DynamoDB's updateItem
  update: (params, obj, options = {}) ->
    debug "update() - " + JSON.stringify obj
    @key.then awsTrans.updateItem.bind(this, params, obj, options)

  ###
  Table Operations
  ###

  # describe
  describe: () ->
    debug 'describe() - ' + @name
    @parent.dynamo.describeTable(TableName: @name).promise()

  # drop
  drop: () ->
    @parent.drop(@name)

module.exports = Table
