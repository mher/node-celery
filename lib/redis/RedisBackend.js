var events = require('events');
var redis  = require('redis');
var util   = require('util');
var debug = process.env.NODE_CELERY_DEBUG === '1' ? console.info : function() {};

function RedisBackend(conf) {
    var self = this;
    self.redis = redis.createClient(conf.RESULT_BACKEND_OPTIONS);

    var backend_ex = self.redis.duplicate();

    self.redis.on('error', function(err) {
        self.emit('error', err);
    });

    self.redis.on('end', function() {
        self.emit('end');
    });

    self.disconnect = function() {
        backend_ex.quit();
        self.redis.quit();
    };

    // store results to emit event when ready
    self.results = {};

    // results prefix
    var key_prefix = 'celery-task-meta-';

    self.redis.on('connect', function() {
        debug('Backend connected...');
        // on redis result..
        self.redis.on('pmessage', function(pattern, channel, data) {
            backend_ex.expire(channel, conf.TASK_RESULT_EXPIRES / 1000);
            var message = JSON.parse(data);
            var taskid = channel.slice(key_prefix.length);
            if (self.results.hasOwnProperty(taskid)) {
                var res = self.results[taskid];
                res.result = message;
                res.emit('ready', res.result);
                delete self.results[taskid];
            } else {
                // in case of incoming messages where we don't have the result object
                self.emit('message', message);
            }
        });
        // subscribe to redis results
        self.redis.psubscribe(key_prefix + '*', () => {
            self.emit('ready');
        });
    });

    self.get = function(taskid, cb) {
        backend_ex.get(key_prefix + taskid, cb);
    }

    return self;
}
util.inherits(RedisBackend, events.EventEmitter);
