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
nullEmptyStrings = (obj) ->
  if isArray obj
    obj.map((value) -> nullEmptyStrings(value))
  else if isObject obj
    Object.keys(obj).reduce((acc, key) ->
      acc[key] = nullEmptyStrings(obj[key])
      acc
    , {})
  else if obj is undefined or (isString(obj) and obj.length is 0)
    null
  else
    obj

toDynamo = (item) ->
  return AWS.DynamoDB.Converter.marshall(nullEmptyStrings(item))

module.exports.toDynamo = toDynamo
