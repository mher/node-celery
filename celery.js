var url = require('url'),
    util = require('util'),
    amqp = require('amqp'),
    redis = require('redis'),
    events = require('events')
    uuid = require('node-uuid');

var createMessage = require('./protocol').createMessage;


debug = process.env.NODE_CELERY_DEBUG === '1' ? util.debug : function() {};

function Configuration(options) {
    var self = this;

    for (var o in options) {
        if (options.hasOwnProperty(o)) {
            self[o.replace(/^CELERY_/, '')] = options[o];
        }
    }

    self.BROKER_URL = self.BROKER_URL || 'amqp://';
    self.BROKER_OPTIONS = self.BROKER_OPTIONS || { url: self.BROKER_URL, heartbeat: 580 }
    self.DEFAULT_QUEUE = self.DEFAULT_QUEUE || 'celery';
    self.DEFAULT_EXCHANGE = self.DEFAULT_EXCHANGE || '';
    self.DEFAULT_EXCHANGE_TYPE = self.DEFAULT_EXCHANGE_TYPE || 'direct';
    self.DEFAULT_ROUTING_KEY = self.DEFAULT_ROUTING_KEY || 'celery';
    self.RESULT_EXCHANGE = self.RESULT_EXCHANGE || 'celeryresults';
    self.TASK_RESULT_EXPIRES = self.TASK_RESULT_EXPIRES * 1000 || 86400000; // Default 1 day
    self.TASK_RESULT_DURABLE = self.TASK_RESULT_DURABLE || true; // Set Durable true by default (Celery 3.1.7)
    self.ROUTES = self.ROUTES || {};

    if (self.RESULT_BACKEND && self.RESULT_BACKEND.toLowerCase() === 'amqp') {
        self.backend_type = 'amqp';
    } else if (self.RESULT_BACKEND && url.parse(self.RESULT_BACKEND)
        .protocol === 'redis:') {
        self.backend_type = 'redis';
    }
}

function Client(conf) {
    var self = this;

    self.conf = new Configuration(conf);

    self.ready = false;
    self.broker_connected = false;
    self.backend_connected = false;

    debug('Connecting to broker...');
    self.broker = amqp.createConnection(
      self.conf.BROKER_OPTIONS
      , {
        defaultExchangeName: self.conf.DEFAULT_EXCHANGE
    });

    if (self.conf.backend_type === 'amqp') {
        self.backend = self.broker;
        self.backend_connected = true;
    } else if (self.conf.backend_type === 'redis') {
        var purl = url.parse(self.conf.RESULT_BACKEND),
            database = purl.pathname.slice(1);
        debug('Connecting to backend...');
        self.backend = redis.createClient(purl.port, purl.hostname);
        if (database) {
            self.backend.select(database);
        }

        if (purl.auth) {
            debug('Authenticating backend...');
            self.backend.auth(purl.auth.split(':')[1]);
            debug('Backend authenticated...');
        }

        self.backend.on('connect', function() {
            debug('Backend connected...');
            self.backend_connected = true;
            if (self.broker_connected) {
                self.ready = true;
                debug('Emiting connect event...');
                self.emit('connect');
            }
        });

        self.backend.on('error', function(err) {
            self.emit('error', err);
        });
    } else {
        self.backend_connected = true;
    }

    self.broker.on('ready', function() {
        debug('Broker connected...');
        self.broker_connected = true;
        if (self.backend_connected) {
            self.ready = true;
            debug('Emiting connect event...');
            self.emit('connect');
        }
    });

    self.broker.on('error', function(err) {
        self.emit('error', err);
    });

    self.broker.on('end', function() {
        self.emit('end');
    });
}

util.inherits(Client, events.EventEmitter);

Client.prototype.createTask = function(name, options, exchange) {
    return new Task(this, name, options, exchange);
};

Client.prototype.end = function() {
    this.broker.disconnect();
    if (this.backend && this.broker !== this.backend) {
        this.backend.quit();
    }
};

Client.prototype.call = function(name /*[args], [kwargs], [options], [callback]*/ ) {
    var args, kwargs, options, callback;
    for (var i = arguments.length - 1; i > 0; i--) {
        if (typeof arguments[i] === 'function') {
            callback = arguments[i];
        } else if (Object.prototype.toString.call(arguments[i]) === '[object Array]') {
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

function Task(client, name, options, exchange) {
    var self = this;

    self.client = client;
    self.name = name;
    self.options = options || {};

    var route = self.client.conf.ROUTES[name],
        queue = route && route.queue;

    self.publish = function (args, kwargs, options, callback) {
        var id = options.id || uuid.v4();

        queue = options.queue || self.options.queue || queue || self.client.conf.DEFAULT_QUEUE;
        var msg = createMessage(self.name, args, kwargs, options, id);
        var pubOptions = {
            'contentType': 'application/json',
            'contentEncoding': 'utf-8'
        };

        if (exchange) {
            exchange.publish(queue, msg, pubOptions, callback);
        } else {
            self.client.broker.publish(queue, msg, pubOptions, callback);
        }

        return new Result(id, self.client);
    };
}

Task.prototype.call = function(args, kwargs, options, callback) {
    var self = this;

    args = args || [];
    kwargs = kwargs || {};
    options = options || self.options || {};

    if (!self.client.ready) {
        self.client.emit('error', 'Client is not ready');
    }
    else {
        return self.publish(args, kwargs, options, callback);
    }
};

function Result(taskid, client) {
    var self = this;

    events.EventEmitter.call(self);
    self.taskid = taskid;
    self.client = client;
    self.result = null;

    if (self.client.conf.backend_type === 'amqp') {
        debug('Subscribing to result queue...');
        self.client.backend.queue(
            self.taskid.replace(/-/g, ''), {
                "arguments": {
                    'x-expires': self.client.conf.TASK_RESULT_EXPIRES
                },
                'durable': self.client.conf.TASK_RESULT_DURABLE
            },

            function (q) {
                q.bind(self.client.conf.RESULT_EXCHANGE, '#');
                q.subscribe(function (message) {
                    if (message.contentType === 'application/x-python-serialize') {
                        console.error('Celery should be configured with json serializer');
                        process.exit(1);
                    }
                    self.result = message;
                    //q.unbind('#');
                    debug('Emiting ready event...');
                    self.emit('ready', message);
                    debug('Emiting task status event...');
                    self.emit(message.status.toLowerCase(), message);
                });
            });
    }
}

util.inherits(Result, events.EventEmitter);

Result.prototype.get = function(callback) {
    var self = this;
    if (callback && self.result === null) {
        self.client.backend.get('celery-task-meta-' + self.taskid, function(err, reply) {
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

exports.createClient = function(config, callback) {
    return new Client(config, callback);
};
