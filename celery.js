var url    = require('url'),
    util   = require('util'),
    amqp   = require('amqp'),
    events = require('events');

var Result = require('./lib/Result');
var Task   = require('./lib/Task');
var RedisBroker  = require('./lib/redis/RedisBroker');
var RedisBackend = require('./lib/redis/RedisBackend');

var createMessage = require('./protocol').createMessage;

var debug = process.env.NODE_CELERY_DEBUG === '1' ? console.info : function() {};

var supportedProtocols = ['amqp', 'amqps', 'redis'];
function getProtocol(kind, options) {
    const protocol = url.parse(options.url).protocol.slice(0, -1);
    if (protocol === 'amqps') {
        protocol = 'amqp';
    }
    if (supportedProtocols.indexOf(protocol) === -1) {
        throw new Error(util.format('Unsupported %s type: %s', kind, protocol));
    }
    debug(kind + ' type: ' + protocol);

    return protocol;
}

function addProtocolDefaults(protocol, options) {
    if (protocol === 'amqp') {
      options.heartbeat = options.heartbeat || 580;
    }
}

function Configuration(options) {
    var self = this;

    for (var o in options) {
        if (options.hasOwnProperty(o)) {
            self[o.replace(/^CELERY_/, '')] = options[o];
        }
    }

    // common
    self.TASK_RESULT_EXPIRES = self.TASK_RESULT_EXPIRES * 1000 || 86400000; // Default 1 day

    // broker
    self.BROKER_OPTIONS = self.BROKER_OPTIONS || {};
    self.BROKER_OPTIONS.url = self.BROKER_URL || 'amqp://';
    self.broker_type = getProtocol('broker', self.BROKER_OPTIONS);
    addProtocolDefaults(self.broker_type, self.BROKER_OPTIONS);

    // backend
    self.RESULT_BACKEND_OPTIONS = self.RESULT_BACKEND_OPTIONS || {};
    if (self.RESULT_BACKEND === self.broker_type) {
      self.RESULT_BACKEND = self.BROKER_URL;
    }
    self.RESULT_BACKEND_OPTIONS.url = self.RESULT_BACKEND || self.BROKER_URL;
    self.backend_type = getProtocol('backend', self.RESULT_BACKEND_OPTIONS);
    addProtocolDefaults(self.backend_type, self.RESULT_BACKEND_OPTIONS);

    self.DEFAULT_QUEUE = self.DEFAULT_QUEUE || 'celery';
    self.DEFAULT_EXCHANGE = self.DEFAULT_EXCHANGE || '';
    self.DEFAULT_EXCHANGE_TYPE = self.DEFAULT_EXCHANGE_TYPE || 'direct';
    self.DEFAULT_ROUTING_KEY = self.DEFAULT_ROUTING_KEY || 'celery';
    self.RESULT_EXCHANGE = self.RESULT_EXCHANGE || 'celeryresults';
    self.IGNORE_RESULT = self.IGNORE_RESULT || false;
    self.TASK_RESULT_DURABLE = undefined !== self.TASK_RESULT_DURABLE ? self.TASK_RESULT_DURABLE : true; // Set Durable true by default (Celery 3.1.7)
    self.ROUTES = self.ROUTES || {};
}

function Client(conf) {
    var self = this;
    self.ready = false;

    self.conf = new Configuration(conf);

    // backend
    if (self.conf.backend_type === 'redis') {
        self.backend = new RedisBackend(self.conf);
        self.backend.on('message', function(msg) {
            self.emit('message', msg);
        });
    } else if (self.conf.backend_type === 'amqp') {
        self.backend = amqp.createConnection(self.conf.RESULT_BACKEND_OPTIONS, {
            defaultExchangeName: self.conf.DEFAULT_EXCHANGE
        });
    }

    self.backend.on('error', function(err) {
        self.emit('error', err);
    });

    // backend ready...
    self.backend.on('ready', function() {
        debug('Connecting to broker...');

        if (self.conf.broker_type === 'redis') {
            self.broker = new RedisBroker(self.conf);
        } else if (self.conf.broker_type === 'amqp') {
            self.broker = amqp.createConnection(self.conf.BROKER_OPTIONS, {
                defaultExchangeName: self.conf.DEFAULT_EXCHANGE
            });
        }

        self.broker.on('error', function(err) {
            self.emit('error', err);
        });

        self.broker.on('end', function() {
            self.emit('end');
        });

        self.broker.on('ready', function() {
            debug('Broker connected...');
            self.ready = true;
            debug('Emiting connect event...');
            self.emit('connect');
        });
    });
}

util.inherits(Client, events.EventEmitter);

Client.prototype.createTask = function(name, options, exchange) {
    return new Task(this, name, options, exchange);
};

Client.prototype.end = function() {
    this.broker.disconnect();
    this.backend.disconnect();
};

Client.prototype.call = function(name /*[args], [kwargs], [options], [callback]*/ ) {
    var args, kwargs, options, callback;
    for (var i = arguments.length - 1; i > 0; i--) {
        if (typeof arguments[i] === 'function') {
            callback = arguments[i];
        } else if (Array.isArray(arguments[i])) {
            args = arguments[i];
        } else if (typeof arguments[i] === 'object') {
            if (options) {
                kwargs = arguments[i];
            } else {
                options = arguments[i];
            }
        }
    }

    var task = this.createTask(name),
        result = task.call(args, kwargs, options);

    if (callback && result) {
        debug('Subscribing to result...');
        result.on('ready', callback);
    }
    return result;
};

exports.createClient = function(config, callback) {
    return new Client(config, callback);
};

exports.createResult = function(taskId, client) {
    return new Result(taskId, client);
};
