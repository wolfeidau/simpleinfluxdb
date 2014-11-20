'use strict';

var qs = require('querystring');
var Hoek = require('hoek');
var Joi = require('joi');
var moment = require('moment')
var request = require('request');
var debug = require('debug')('simpleinfluxdb');

var periodMap = {
    "min": "m",
    "hour": "h",
    "day": "d"
};

var schema = {
    opts: {
        interval: Joi.string(),
        'function': Joi.valid('sum', 'max', 'mean', 'average', 'stddev', 'count'),
        start: Joi.number(),
        end: Joi.number()
    },
    data: Joi.array().min(1).includes(
        Joi.object().keys({
            t: Joi.date(),
            v: Joi.number()
        })
    )
};

var defaults = {
    hostname: 'localhost',
    database: 'mydb',
    username: 'root',
    password: 'root',
    protocol: 'http',
    maxSockets: 25
};

function SimpleInfluxDB(opts) {
    opts = opts || {};
    this.config = Hoek.applyToDefaults(defaults, opts);

    this.baseUrl = this.config.protocol + '://' + this.config.hostname + '/db/' + this.config.database;

    // the agent which will be used for this instance to ensure keep alive is honored.
    var Agent = require(this.config.protocol).Agent;
    this.agent = new Agent({maxSockets: this.config.maxSockets});
}

SimpleInfluxDB.prototype._callApi = function (method, path, body, cb) {

    this.headers['connection'] = 'keep-alive';

    var options = {
        url: url.parse(this.baseUrl + path),
        method: method,
        headers: this.headers,
        body: JSON.stringify(body),
        agent: this.agent,
        json: true
    };

    request(options, cb);
};

SimpleInfluxDB.prototype.read = function (start, end, opts, cb) {

    var self = this;

    Joi.validate(opts, schema.opts, function (err, value) {

        if (err) {
            return cb(err);
        }

        self._remapPeriod(opts.interval, function (err, period) {

            if (err) {
                return cb(err);
            }

            var whereClause = 'time > ' + moment().subtract(1, 'days').getTime() + 'and time < ' + moment().getTime(); // because i hate js dates

            if (opts.start && opts.end) {
                // use the supplied params
                whereClause = 'time > ' + opts.start + ' and time < ' + opts.end;
            }

            // if there is an interval we are doing a rollup
            // TODO: If we are retieving rolled up data we are going to need to adjust the key name to cater for that.
            var query = 'select ' + opts['function'] + '(value) from ' + key + ' where ' + whereClause + ' group by time(' + period + ')';

            debug('read', 'query', query);

            self._callApi('GET', '/series?' + qs.stringify({
                u: self.config.username,
                p: self.config.password,
                'q': query
            }), null, cb)

        });

    });


};

SimpleInfluxDB.prototype.write_key = function (series_key, data, cb) {

    var self = this;

    var payload = {
        "name": series_key,
        "columns": ["time", "value"],
        "points": []
    };

    Joi.validate(data, schema.data, function (err, value) {

        if (err) {
            return cb(err);
        }

        value.forEach(function (element) {
            payload.points.push([element.t.getTime(), element.v]);
        });

        debug('write', '_callApi', payload);

        self._callApi('POST', '/series?' + qs.stringify({
            u: self.config.username,
            p: self.config.password
        }), payload, cb)

    });

};

SimpleInfluxDB.prototype._remapPeriod = function (period, cb) {

    // if it isn't set then just callback
    if (!period) {
        return cb();
    }

    if (period.match(/\d+(day|month|hour)/)) {
        Object.keys(periodMap).map(function (value) {
            period = period.replace(value, periodMap[value]);
        });
        cb(null, period);
    } else {
        cb(Error('Invalid period'));
    }
};

exports = module.exports = SimpleInfluxDB;