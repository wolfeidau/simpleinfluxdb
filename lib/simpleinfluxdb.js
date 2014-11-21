'use strict';

var qs = require('querystring');
var url = require('url');
var Hoek = require('hoek');
var Joi = require('joi');
var moment = require('moment');
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
        'function': Joi.string().regex(/(sum|max|mean|average|stddev|count)/),
        start: Joi.date(),
        end: Joi.date(),
        key: Joi.string()
    },
    data: Joi.array().min(1)
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

    this.headers = {connection: 'keep-alive'};

    // the agent which will be used for this instance to ensure keep alive is honored.
    var Agent = require(this.config.protocol).Agent;
    this.agent = new Agent({maxSockets: this.config.maxSockets});
}

SimpleInfluxDB.prototype._callApi = function (method, path, body, cb) {

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

    debug('read', 'start', start, 'end', end, 'opts', opts);

    var self = this;

    var err = Joi.validate(opts, schema.opts);

    if (err) {
        return cb(err);
    }

    self._remapPeriod(opts.interval, function (err, period) {

        if (err) {
            return cb(err);
        }

        var whereClause = 'time > ' + moment().subtract(1, 'days').unix() + 's and time < ' + moment().unix() + 's'; // because i hate js dates

        if (start && end) {
            debug('read', 'where', 'start', start, 'end', end);
            // use the supplied params
            whereClause = 'time > ' + moment(start).unix() + 's and time < ' + moment(end).unix() + 's';
        }

        if (start && !end) {
            debug('read', 'where', 'start', start);
            // use the supplied params
            whereClause = 'time > ' + start;
        }

        // if there is an interval we are doing a rollup
        // TODO: If we are relieving rolled up data we are going to need to adjust the key name to cater for that.
        var query = 'select ' + opts['function'] + '(value) from ' + opts.key + ' where ' + whereClause + ' group by time(' + period + ')';

        debug('read', 'query', query);
        console.log('read', 'query', query);

        self._callApi('GET', '/series?' + qs.stringify({
            u: self.config.username,
            p: self.config.password,
            'q': query
        }), null, function resultTransformer(err, result) {
            // this should be separated out at the moment it just
            // transforms the output from arrays of arrays to arrays of objects
            if (err) {
                return cb(err, result);
            }

            if (result.body instanceof Array) {

                if (result.body.length == 0) {
                    return cb(err, result);
                }

                var data = result.body[0].points.map(function (val) {
                    return {t: val[0], v: val[1]}
                });

                result.body[0].data = data;

                delete result.body[0].points;
            }

            cb(err, result);

        })

    });


};

SimpleInfluxDB.prototype.write_key = function (series_key, data, cb) {

    var self = this;

    var payload = {
        "name": series_key,
        "columns": ["time", "value"],
        "points": []
    };

    var err = Joi.validate(data, schema.data);

    if (err) {
        return cb(err);
    }

    for (var i=0; i<data.length; ++i) {
        if (data[i].t instanceof Date && !isNaN(data[i].t.valueOf())) {
            payload.points.push([data[i].t.getTime(), data[i].v]);
        } else {
            return cb(Error('invalid date value in data'))
        }
    }

    debug('write', '_callApi', payload);

    self._callApi('POST', '/series?' + qs.stringify({
        u: self.config.username,
        p: self.config.password
    }), [payload], cb)

};

SimpleInfluxDB.prototype._remapPeriod = function (period, cb) {

    // if it isn't set then just callback
    if (!period) {
        return cb();
    }

    if (period.match(/\d+(day|hour|min)/)) {
        Object.keys(periodMap).map(function (value) {
            period = period.replace(value, periodMap[value]);
        });
        cb(null, period);
    } else {
        cb(Error('Invalid period'));
    }
};

exports = module.exports = SimpleInfluxDB;