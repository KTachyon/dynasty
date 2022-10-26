# Main Dynasty Class

aws = require('aws-sdk')
isObject = require('lodash/isObject')
isString = require('lodash/isString')
debug = require('debug')('dynasty')
{Table} = require('./lib')

class Dynasty

  constructor: (credentials, url) ->
    debug "dynasty constructed."
    credentials.region = credentials.region || 'us-east-1'

    # Lock API version
    credentials.apiVersion = '2012-08-10'

    if url and isString url
      debug "connecting to local dynamo at #{url}"
      credentials.endpoint = new aws.Endpoint url

    @dynamo = new aws.DynamoDB(credentials)
    @name = 'Dynasty'
    @tables = {}

  # Given a name, return a Table object
  table: (name) ->
    @tables[name] = @tables[name] || new Table this, name

  ###
  Table Operations
  ###

  # describe
  describe: (name) ->
    debug "describe() - #{name}"
    @dynamo.describeTable(TableName: name).promise()

  # Useful if for example you are implementing triggers and you are getting
  # raw dynamo JSON data.
  convertFromDynamo: (dynamoData) ->
    lib['data-translators'].fromDynamo(dynamoData)

  convertToDynamo: (data) ->
    lib['data-translators'].toDynamo(data)

module.exports = (credentials, url) -> new Dynasty(credentials, url)
