chai = require('chai')
expect = require('chai').expect
Chance = require('chance')
chance = new Chance()
_ = require('lodash')
dataTrans = require('../lib/lib')['data-translators']

describe 'toDynamo()', () ->

  it 'looks right when given a number', () ->
    num = chance.integer()
    converted = dataTrans.toDynamo foo: num
    expect(converted).to.be.an 'object'
    expect(converted).to.deep.equal
      foo:
        N: num.toString()

  it 'looks right when given a string', () ->
    str = chance.string()
    converted = dataTrans.toDynamo foo: str
    expect(converted).to.be.an 'object'
    expect(converted).to.deep.equal
      foo:
        S: str

  it 'converts an empty string to null', () ->
    expect(dataTrans.toDynamo(foo: '')).to.deep.equal
      foo:
        NULL: true

  it 'allows non empty strings', () ->
    expect(dataTrans.toDynamo(foo: ' ')).to.deep.equal
      foo:
        S: ' '

  it 'looks right when given a long string', () ->
    str = chance.string
      length: 1025
    converted = dataTrans.toDynamo foo: str
    expect(converted).to.be.an 'object'
    expect(converted).to.deep.equal
      foo:
        S: str

  it 'should convert objects to Maps', () ->
    expect(dataTrans.toDynamo(moo: {foo: 'bar'})).to.deep.equal(moo: {M: {foo: {S: 'bar'}}})

  it 'looks right when given an array of numbers', () ->
    arr = moo: [0, 1, 2, 3]
    converted = dataTrans.toDynamo arr
    expect(converted).to.be.an 'object'
    expect(converted).to.deep.equal
      moo:
        L: [
          N: '0'
        ,
          N: '1'
        ,
          N: '2'
        ,
          N: '3'
        ]

  it 'looks right when given an array of strings', () ->
    arr = moo: ['woof', 'miow', 'foo']
    converted = dataTrans.toDynamo arr
    expect(converted).to.be.an 'object'
    expect(converted).to.deep.equal
      moo:
        L: [
          S: 'woof'
        ,
          S: 'miow'
        ,
          S: 'foo'
        ]

  it 'looks right when given an array of objects', () ->
    arr = moo: [{foo: 'bar'}, {bar: 'foo'}]
    converted = dataTrans.toDynamo arr
    expect(converted).to.be.an 'object'
    expect(converted).to.deep.equal(moo: { L: [{M: {foo: {S: 'bar'}}},{M: {bar: {S: 'foo'}}}]})

  it 'looks right when given an array of nested objects', () ->
    arr = moo: [{foo: [1,2,3]}, {bar: {amazon: 'aws'}}]
    converted = dataTrans.toDynamo arr
    expect(converted).to.be.an 'object'
    expect(converted).to.deep.equal(
      moo:
        L:
          [
            M:
              foo:
                L: [
                  N:'1'
                ,
                  N: '2'
                ,
                  N: '3'
                ]
          ,
            M:
              bar:
                M:
                  amazon:
                    S: 'aws'
          ]
    )

  it 'converts an empty array to a list', () ->
    expect(dataTrans.toDynamo(foo: [])).to.deep.equal(
      foo:
        L: []
    )

  it 'converts an array of strings with dupes to a list', () ->
    expect(dataTrans.toDynamo(foo: ['1', '1'])).to.deep.equal(
      foo:
        L: [
          S: '1'
        ,
          S: '1'
        ]
    )

  it 'deals with an array with empty strings', () ->
    expect(dataTrans.toDynamo(foo: ['', ' '])).to.deep.equal(
      foo:
        L: [
          NULL: true
        ,
          S: ' '
        ]
    )

  it 'converts an array of numbers with duplicates to a list', () ->
    expect(dataTrans.toDynamo(foo: [1, 1])).to.deep.equal(
      foo:
        L: [
          N: '1'
        ,
          N: '1'
        ]
    )

  it 'supports null values', () ->
    expect(dataTrans.toDynamo(bar: {foo: null})).to.deep.equal
      bar:
        'M':
          foo:
            'NULL': true

  it 'converts undefined to null', () ->
    expect(dataTrans.toDynamo(bar: {foo: undefined})).to.deep.equal
      bar:
        'M':
          foo:
            'NULL': true

describe 'fromDynamo()', () ->

  it 'converts dynamo NULLs to javascript nulls' , () ->
    expect(dataTrans.fromDynamo(foo: {NULL: true})).to.deep.equal foo: null

  it 'converts string lists correctly', () ->
    dynamoData =
      foo:
        L: [
          S: 'foo'
        ,
          S: 'bar'
        ]
    expect(dataTrans.fromDynamo(dynamoData)).to.deep.equal(foo: ['foo', 'bar'])

  it 'converts numbered lists correctly',() ->
    dynamoData =
      bar:
        M:
          foo:
            L: [
              N: 0
            ,
              N: 1
            ]
    expect(dataTrans.fromDynamo(dynamoData)).to.deep.equal(bar: {foo: [0, 1]})
