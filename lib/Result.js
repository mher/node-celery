var events = require('events');
var util   = require('util');
var debug = process.env.NODE_CELERY_DEBUG === '1' ? console.info : function() {};


function Result(taskid, client) {
    var self = this;

    events.EventEmitter.call(self);
    self.taskid = taskid;
    self.client = client;
    self.result = null;

    if (self.client.conf.backend_type === 'amqp' && !self.client.conf.IGNORE_RESULT) {
        debug('Subscribing to result queue...');
        self.client.backend.queue(
            self.taskid.replace(/-/g, ''), {
                "arguments": {
                    'x-expires': self.client.conf.TASK_RESULT_EXPIRES
                },
                'durable': self.client.conf.TASK_RESULT_DURABLE,
                'closeChannelOnUnsubscribe': true
            },

            function (q) {
                q.bind(self.client.conf.RESULT_EXCHANGE, '#');
                var ctag;
                q.subscribe(function (message) {
                    if (message.contentType === 'application/x-python-serialize') {
                        console.error('Celery should be configured with json serializer');
                        process.exit(1);
                    }
                    self.result = message;
                    q.unsubscribe(ctag);
                    debug('Emiting ready event...');
                    self.emit('ready', message);
                    debug('Emiting task status event...');
                    self.emit(message.status.toLowerCase(), message);
                }).addCallback(function(ok) { ctag = ok.consumerTag; });
            });
    }
}

util.inherits(Result, events.EventEmitter);

Result.prototype.get = function(callback) {
    var self = this;
    if (callback && self.result === null) {
        self.client.backend.get(self.taskid, function(err, reply) {
            self.result = JSON.parse(reply);
            callback(self.result);
        });
    } else {
        if (callback) {
            callback(self.result);
        }
        return self.result;
    }
};

module.exports = Result;
