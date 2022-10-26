AWS = require('aws-sdk')
isArray = require('lodash/isArray')
isObject = require('lodash/isObject')
isString = require('lodash/isString')

fromDynamo = (dbObj) ->
  if isArray dbObj
    dbObj.map((item) -> AWS.DynamoDB.Converter.unmarshall(item))
  else if isObject dbObj
    AWS.DynamoDB.Converter.unmarshall(dbObj)
  else
    dbObj

module.exports.fromDynamo = fromDynamo

# WARN: Legacy from old dynasty, we don't want to break existing usage
nullUndefined = (obj) ->
  if isArray obj
    obj.map((value) -> nullUndefined(value))
  else if isObject obj
    Object.keys(obj).reduce((acc, key) ->
      acc[key] = nullUndefined(obj[key])
      acc
    , {})
  else if obj is undefined
    null
  else
    obj

toDynamo = (item) ->
  return AWS.DynamoDB.Converter.marshall(nullUndefined(item), convertEmptyValues: true)

module.exports.toDynamo = toDynamo
