var events = require('events');
var redis  = require('redis');
var uuid   = require('uuid');
var util   = require('util');
var debug = process.env.NODE_CELERY_DEBUG === '1' ? console.info : function() {};

function RedisBroker(conf) {
    var self = this;
    self.redis = redis.createClient(conf.BROKER_OPTIONS);

    self.end = function() {
        self.redis.end(true);
    };

    self.disconnect = function() {
        self.redis.quit();
    };

    self.redis.on('connect', function() {
        self.emit('ready');
    });

    self.redis.on('error', function(err) {
        self.emit('error', err);
    });

    self.redis.on('end', function() {
        self.emit('end');
    });

    self.publish = function(queue, message, options, callback, id) {
        var payload = {
            body: new Buffer(message).toString('base64'),
            headers: {},
            'content-type': options.contentType,
            'content-encoding': options.contentEncoding,
            properties: {
                body_encoding: 'base64',
                correlation_id: id,
                delivery_info: {
                    exchange: queue,
                    priority: 0,
                    routing_key: queue
                },
                delivery_mode: 2, // No idea what this means
                delivery_tag: uuid.v4(),
                reply_to: uuid.v4()
            }
        };
        self.redis.lpush(queue, JSON.stringify(payload));
    };

    return self;
}
util.inherits(RedisBroker, events.EventEmitter);
