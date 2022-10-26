chai = require('chai')
expect = chai.expect
chaiAsPromised = require('chai-as-promised')
chai.use(chaiAsPromised)
chance = require('chance').Chance()
lib = require('../lib/lib')["aws-translators"]
sinon = require('sinon')

describe 'aws-translators', () ->
  describe '#getKeySchema', () ->
    it 'should parse out a hash key from an aws response', () ->
      hashKeyName = chance.word()
      result = lib.getKeySchema(
        Table:
          KeySchema:[
            AttributeName: hashKeyName
            KeyType: 'HASH'
          ]
          AttributeDefinitions: [
            AttributeName: hashKeyName
            AttributeType: 'N'
          ]
      )

      expect(result).to.have.property('hashKeyName')
      expect(result.hashKeyName).to.equal(hashKeyName)

      expect(result).to.have.property('hashKeyType')
      expect(result.hashKeyType).to.equal('N')

    it 'should parse out a range key from an aws response', () ->
      hashKeyName = chance.word()
      rangeKeyName = chance.word()
      result = lib.getKeySchema(
        Table:
          KeySchema:[
            {
              AttributeName: hashKeyName
              KeyType: 'HASH'
            },
            {
              AttributeName: rangeKeyName
              KeyType: 'RANGE'
            }
          ]
          AttributeDefinitions: [
            {
              AttributeName: hashKeyName
              AttributeType: 'S'
            },
            {
              AttributeName: rangeKeyName
              AttributeType: 'B'
            }
          ]
      )

  describe '#deleteItem', () ->

    dynastyTable = null
    sandbox = null

    beforeEach () ->
      sandbox = sinon.sandbox.create()
      dynastyTable =
        name: chance.name()
        parent:
          dynamo: {
            deleteItem: (params) ->
              promise: -> Promise.resolve('lol')
          }

    afterEach () ->
      sandbox.restore()

    it 'should return an object', () ->
      promise = lib.deleteItem.call(dynastyTable, 'foo', null,
        hashKeyName: 'bar'
        hashKeyType: 'S'
      )
      expect(promise).to.be.an('object')

    it 'should return a promise', () ->
      promise = lib.deleteItem.call(dynastyTable, 'foo', null,
        hashKeyName: 'bar'
        hashKeyType: 'S'
      )
      promise.then((d) -> expect(d).to.equal('lol'))

    it 'should call deleteItem of aws', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "deleteItem")
      lib.deleteItem.call(dynastyTable, 'foo', null,
        hashKeyName: 'bar'
        hashKeyType: 'S'
      )
      expect(dynastyTable.parent.dynamo.deleteItem.calledOnce)

    it 'should send the table name to AWS', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "deleteItem")

      promise = lib.deleteItem.call(dynastyTable, 'foo', null,
        hashKeyName: 'bar'
        hashKeyType: 'S'
      )

      promise.then () ->
        expect(dynastyTable.parent.dynamo.deleteItem.calledOnce)
        params = dynastyTable.parent.dynamo.deleteItem.getCall(0).args[0]
        expect(params.TableName).to.equal(dynastyTable.name)

    it 'should send the hash key to AWS', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "deleteItem")

      promise = lib.deleteItem.call(dynastyTable, 'foo', null,
        hashKeyName: 'bar'
        hashKeyType: 'S'
      )

      expect(dynastyTable.parent.dynamo.deleteItem.calledOnce)
      params = dynastyTable.parent.dynamo.deleteItem.getCall(0).args[0]
      expect(params.Key).to.include.keys('bar')
      expect(params.Key.bar).to.include.keys('S')
      expect(params.Key.bar.S).to.equal('foo')

    it 'should send the hash and range key to AWS', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "deleteItem")

      promise = lib.deleteItem.call(
        dynastyTable,
          hash: 'lol'
          range: 'rofl',
        null,
          hashKeyName: 'bar'
          hashKeyType: 'S'
          rangeKeyName: 'foo'
          rangeKeyType: 'S')

      expect(dynastyTable.parent.dynamo.deleteItem.calledOnce)
      params = dynastyTable.parent.dynamo.deleteItem.getCall(0).args[0]

      expect(params.Key).to.include.keys('bar')
      expect(params.Key.bar).to.include.keys('S')
      expect(params.Key.bar.S).to.equal('lol')

      expect(params.Key).to.include.keys('foo')
      expect(params.Key.foo).to.include.keys('S')
      expect(params.Key.foo.S).to.equal('rofl')

  describe '#batchGetItem', () ->

    dynastyTable = null
    sandbox = null

    beforeEach () ->
      tableName = chance.name()
      sandbox = sinon.sandbox.create()
      dynastyTable =
        name: tableName
        parent:
          dynamo:
            batchGetItem: (params) ->
              result = {}
              result.Responses = {}
              result.Responses[tableName] = [
                { foo: S: "bar" },
                foo: S: "baz"
                bazzoo: N: 123
              ]
              promise: -> Promise.resolve(result)

    afterEach () ->
      sandbox.restore()

    it 'should return a sane response', () ->
      lib.batchGetItem
        .call dynastyTable, ['bar', 'baz'],
          hashKeyName: 'foo'
          hashKeyType: 'S'
        .then (data) ->
          expect(data).to.deep.equal([
            { foo: 'bar' },
            foo: 'baz'
            bazzoo: 123])

  describe '#getItem', () ->

    dynastyTable = null
    sandbox = null

    beforeEach () ->
      sandbox = sinon.sandbox.create()
      dynastyTable =
        name: chance.name()
        parent:
          dynamo: {
            getItem: (params) ->
              promise: -> Promise.resolve(Item: rofl: S: 'lol')
          }

    afterEach () ->
      sandbox.restore()

    it 'should return an object', () ->
      promise = lib.getItem.call(dynastyTable, 'foo', null,
        hashKeyName: 'bar'
        hashKeyType: 'S'
      )

      expect(promise).to.be.an('object')

    it 'should return a promise', () ->
      lib.getItem
        .call dynastyTable, 'foo', null,
          hashKeyName: 'bar'
          hashKeyType: 'S'
        .then (data) ->
          expect(data).to.deep.equal(rofl: 'lol')

    it 'should call getItem of aws', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "getItem")
      lib.getItem.call dynastyTable, 'foo', null,
        hashKeyName: 'bar'
        hashKeyType: 'S'

      expect(dynastyTable.parent.dynamo.getItem.calledOnce)
      expect(dynastyTable.parent.dynamo.getItem.getCall(0).args[0].TableName).to.equal(dynastyTable.name)

    it 'should send the table name to AWS', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "getItem")

      lib.getItem
        .call dynastyTable, 'foo', null,
          hashKeyName: 'bar'
          hashKeyType: 'S'
        .then () ->
          expect(dynastyTable.parent.dynamo.getItem.calledOnce)
          params = dynastyTable.parent.dynamo.getItem.getCall(0).args[0]
          expect(params.TableName).to.equal(dynastyTable.name)

    it 'should send the hash key to AWS', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "getItem")

      promise = lib.getItem.call dynastyTable, 'foo', null,
        hashKeyName: 'bar'
        hashKeyType: 'S'

      expect(dynastyTable.parent.dynamo.getItem.calledOnce)
      params = dynastyTable.parent.dynamo.getItem.getCall(0).args[0]
      expect(params.Key).to.include.keys('bar')
      expect(params.Key.bar).to.include.keys('S')
      expect(params.Key.bar.S).to.equal('foo')

    it 'should send the hash and range key to AWS', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "getItem")

      promise = lib.getItem.call(
        dynastyTable,
          hash: 'lol'
          range: 'rofl',
        null,
          hashKeyName: 'bar'
          hashKeyType: 'S'
          rangeKeyName: 'foo'
          rangeKeyType: 'S')

      expect(dynastyTable.parent.dynamo.getItem.calledOnce)
      params = dynastyTable.parent.dynamo.getItem.getCall(0).args[0]

      expect(params.Key).to.include.keys('bar')
      expect(params.Key.bar).to.include.keys('S')
      expect(params.Key.bar.S).to.equal('lol')

      expect(params.Key).to.include.keys('foo')
      expect(params.Key.foo).to.include.keys('S')
      expect(params.Key.foo.S).to.equal('rofl')

  describe '#scan', () ->

    dynastyTable = null
    sandbox = null

    beforeEach () ->
      sandbox = sinon.sandbox.create()
      dynastyTable =
        name: chance.name()
        parent:
          dynamo: {
            scan: (params) ->
              promise: -> Promise.resolve Items: [{ rofl: S: 'lol' }]
          }

    afterEach () ->
      sandbox.restore()

    it 'should return an object', () ->
      promise = lib.scan.call(dynastyTable, 'foo', null,
        hashKeyName: 'bar'
        hashKeyType: 'S'
      )

      expect(promise).to.be.an('object')

    it 'should return a promise', () ->
      lib.scan
      .call dynastyTable, 'foo', null,
        hashKeyName: 'bar'
        hashKeyType: 'S'
      .then (data) ->
        expect(data).to.deep.equal([rofl: 'lol'])

    it 'should call scan of aws', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "scan")
      lib.scan.call dynastyTable, 'foo', null,
        hashKeyName: 'bar'
        hashKeyType: 'S'

      expect(dynastyTable.parent.dynamo.scan.calledOnce)
      expect(dynastyTable.parent.dynamo.scan.getCall(0).args[0].TableName).to.equal(dynastyTable.name)

  describe '#queryByHashKey', () ->

    dynastyTable = null
    sandbox = null

    beforeEach () ->
      sandbox = sinon.sandbox.create()
      dynastyTable =
        name: chance.name()
        parent:
          dynamo: {
            query: (params) ->
              promise: -> 
                Promise.resolve Items: [{
                  foo: {S: 'bar'},
                  bar: {S: 'baz'}
                }]
          }

    afterEach () ->
      sandbox.restore()

    it 'should translate the response', () ->

      lib.queryByHashKey
        .call dynastyTable, 'bar',
          hashKeyName: 'foo'
          hashKeyType: 'S'
          rangeKeyName: 'bar'
          rangeKeyType: 'S'
        .then (data) ->
          expect(data).to.deep.equal [
            foo: 'bar'
            bar: 'baz'
          ]

    it 'should call query', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "query")

      lib.queryByHashKey.call dynastyTable, 'bar',
        hashKeyName: 'foo'
        hashKeyType: 'S'
        rangeKeyName: 'bar'
        rangeKeyType: 'S'

      expect(dynastyTable.parent.dynamo.query.calledOnce)
      expect(dynastyTable.parent.dynamo.query.getCall(0).args[0]).to.include.keys('TableName', 'KeyConditions')

    it 'should send the table name and hash key to AWS', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "query")
      promise = lib.queryByHashKey.call dynastyTable, 'bar',
        hashKeyName: 'foo'
        hashKeyType: 'S'
        rangeKeyName: 'bar'
        rangeKeyType: 'S'

      expect(dynastyTable.parent.dynamo.query.calledOnce)
      params = dynastyTable.parent.dynamo.query.getCall(0).args[0]
      expect(params.TableName).to.equal(dynastyTable.name)
      expect(params.KeyConditions.foo.ComparisonOperator).to.equal('EQ')
      expect(params.KeyConditions.foo.AttributeValueList[0].S)
        .to.equal('bar')

  describe '#putItem', () ->

    dynastyTable = null
    sandbox = null

    beforeEach () ->
      sandbox = sinon.sandbox.create()
      dynastyTable =
        name: chance.name()
        parent:
          dynamo: {
            putItem: (params) ->
              promise: -> Promise.resolve('lol')
          }

    afterEach () ->
      sandbox.restore()

    it 'should return an object', () ->
      promise = lib.putItem.call(dynastyTable, foo: 'bar', null)

      expect(promise).to.be.an('object')

    it 'should return a promise', () ->
      lib.putItem
        .call(dynastyTable, foo: 'bar', null)
        .then (data) ->
          expect(data).to.equal('lol')

    it 'should call putItem of aws', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "putItem")

      lib.putItem.call(dynastyTable, foo: 'bar', null)

      expect(dynastyTable.parent.dynamo.putItem.calledOnce)
      expect(dynastyTable.parent.dynamo.putItem.getCall(0).args[0]).to.include.keys('Item', 'TableName')

    it 'should send the table name to AWS', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "putItem")

      lib.putItem
        .call(dynastyTable, foo: 'bar', null)
        .then () ->
          expect(dynastyTable.parent.dynamo.putItem.calledOnce)
          params = dynastyTable.parent.dynamo.putItem.getCall(0).args[0]
          expect(params.TableName).to.equal(dynastyTable.name)

    it 'should send the translated object to AWS', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "putItem")

      promise = lib.putItem.call dynastyTable, foo: 'bar', null

      expect(dynastyTable.parent.dynamo.putItem.calledOnce)
      params = dynastyTable.parent.dynamo.putItem.getCall(0).args[0]
      expect(params.Item).to.be.an('object')
      expect(params.Item.foo).to.be.an('object')
      expect(params.Item.foo.S).to.equal('bar')

  describe '#updateItem', () ->

    dynastyTable = null
    sandbox = null
    updateSpy = null

    beforeEach () ->
      sandbox = sinon.sandbox.create()
      dynastyTable =
        name: chance.name()
        parent:
          dynamo: {
            updateItem: (params) ->
              promise: -> Promise.resolve('lol')
          }
      updateSpy = sandbox.spy(dynastyTable.parent.dynamo, "updateItem")

    afterEach () ->
      sandbox.restore()

    it 'should automatically setup ExpressionAttributeNames mapping', () ->
      promise = lib.updateItem.call(dynastyTable, {}, foo: 'bar', null,
        hashKeyName: 'bar'
        hashKeyType: 'S'
      )
      expect(updateSpy.calledOnce)
      expect(updateSpy.getCall(0).args[0].ExpressionAttributeNames).to.eql({"#foo": 'foo'})

    it 'should allow incrementing numbers with option incrementNumber', () ->
      promise = lib.updateItem.call(dynastyTable, {}, foo: 1, {incrementNumber: true},
        hashKeyName: 'bar'
        hashKeyType: 'S'
      )
      expect(updateSpy.calledOnce)
      params = updateSpy.getCall(0).args[0]
      expect(params.ExpressionAttributeValues).to.eql({":foo": {'N': '1'}})
      expect(params.UpdateExpression).to.eql("ADD #foo :foo")

    it 'should allow decrementing numbers with option incrementNumber', () ->
      promise = lib.updateItem.call(dynastyTable, {}, foo: -5, {incrementNumber: true},
        hashKeyName: 'bar'
        hashKeyType: 'S'
      )
      expect(updateSpy.calledOnce)
      params = updateSpy.getCall(0).args[0]
      expect(params.ExpressionAttributeValues).to.eql({":foo": {'N': '-5'}})
      expect(params.UpdateExpression).to.eql("ADD #foo :foo")
