'use strict';

var debug = require('debug')('test:simpleinfluxdb');
var SimpleInfluxDB = require('../index');
var expect = require('chai').expect;

describe('simpleinfluxdb', function () {

    var sdb = new SimpleInfluxDB();

    it('simpleinfluxdb should load defaults', function () {


        debug('simpleinfluxdb', sdb);

        // check the defaults.
        expect(sdb.config.hostname).to.equal('localhost');
        expect(sdb.config.database).to.equal('mydb');
        expect(sdb.config.username).to.equal('root');
        expect(sdb.config.password).to.equal('root');

    });

    it('should remap period names', function (done) {

        sdb._remapPeriod('1day', function (err, res) {
            expect(err).to.not.exist;
            expect(res).to.equal('1d');
            done();
        })

    });

    it('should read and raise a function error', function (done) {
        var sdbread = new SimpleInfluxDB();

        SimpleInfluxDB.prototype._callApi = function (method, path, body, cb) {
            cb(null, {})
        };

        sdbread.read(new Date().getTime(), new Date().getTime(), {'function': 'woot'}, function (err, data) {
            expect(err).to.exist;
            done();
        })

    });

    it('should read and raise a number error if start or end is a string', function (done) {
        var sdbread = new SimpleInfluxDB();

        SimpleInfluxDB.prototype._callApi = function (method, path, body, cb) {
            cb(null, {})
        };

        sdbread.read(new Date().getTime(), new Date().getTime(), {
            'function': 'count',
            start: 'hello',
            end: 'foo'
        }, function (err, data) {
            expect(err).to.exist;
            sdbread.read(new Date().getTime(), new Date().getTime(), {
                'function': 'count',
                start: new Date().getTime(),
                end: 'foo'
            }, function (err, data) {
                expect(err).to.exist;
                done();
            })
        })

    });

    it('should read and transform result', function (done) {
        var sdbread = new SimpleInfluxDB();

        SimpleInfluxDB.prototype._callApi = function (method, path, body, cb) {
            cb(null, {body: [{points: [[1416451118000, 23.559637793913357]]}]})
        };

        sdbread.read(new Date().getTime(), new Date().getTime(), {
            'key': '123_SOMEYO_1_30',
            'function': 'count',
            start: 1416554706524,
            interval: '1min'
        }, function (err, data) {
            expect(data).to.exist;
            debug('read', data);
            expect(data.body.length).to.equal(1);
            expect(data.body[0].data.length).to.equal(1);
            expect(data.body[0].data[0].t).to.deep.equal(new Date(1416451118000));
            expect(data.body[0].data[0].v).to.equal(23.559637793913357);
            done();
        })
    });

    it('should write and return an array error', function (done) {
        var sdbread = new SimpleInfluxDB();

        SimpleInfluxDB.prototype._callApi = function (method, path, body, cb) {
            cb(null, {})
        };

        sdbread.write_key('some_key', {}, function (err, res) {
            expect(err).to.exist;
            done();
        })
    });

    it('should write and return a date error', function (done) {
        var sdbread = new SimpleInfluxDB();

        var testPath = "";

        SimpleInfluxDB.prototype._callApi = function (method, path, body, cb) {
            testPath = path;
            cb(null, {})
        };

        sdbread.write_key('some_key', [{t: 'woot', v: 123.1}], function (err, res) {
            expect(err).to.exist;
            expect(testPath).to.equal("");
            done();
        })
    });

    it('should write and return a valid path', function (done) {
        var sdbread = new SimpleInfluxDB();

        var testPath = "";
        var testBody = {};

        SimpleInfluxDB.prototype._callApi = function (method, path, body, cb) {
            testPath = path;
            testBody = body;
            cb(null, {})
        };

        sdbread.write_key('some_key', [{t: new Date(1416451118000), v: 123.1}], function (err, res) {
            expect(testPath).to.equal("/series?u=root&p=root");
            expect(testBody[0].name).to.equal('some_key');
            expect(testBody[0].columns).to.deep.equal(['time', 'value']);
            expect(testBody[0].points).to.deep.equal(
                [[1416451118000, 123.1]]
            );
            done();
        })
    });
});