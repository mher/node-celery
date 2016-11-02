var url = require('url'),
    util = require('util'),
    amqp = require('amqp'),
    redis = require('redis'),
    events = require('events'),
    uuid = require('node-uuid');

var createMessage = require('./protocol').createMessage;

var debug = process.env.NODE_CELERY_DEBUG === '1' ? console.info : function() {};

var supportedProtocols = ['amqp', 'amqps', 'redis'];
function checkProtocol(kind, protocol) {
    if (supportedProtocols.indexOf(protocol) === -1) {
        throw new Error(util.format('Unsupported %s type: %s', kind, protocol));
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
    self.BROKER_URL = self.BROKER_URL || 'amqp://';
    self.BROKER_OPTIONS = self.BROKER_OPTIONS || { url: self.BROKER_URL, heartbeat: 580 };
    self.DEFAULT_QUEUE = self.DEFAULT_QUEUE || 'celery';
    self.DEFAULT_EXCHANGE = self.DEFAULT_EXCHANGE || '';
    self.DEFAULT_EXCHANGE_TYPE = self.DEFAULT_EXCHANGE_TYPE || 'direct';
    self.DEFAULT_ROUTING_KEY = self.DEFAULT_ROUTING_KEY || 'celery';
    self.RESULT_EXCHANGE = self.RESULT_EXCHANGE || 'celeryresults';
    self.IGNORE_RESULT = self.IGNORE_RESULT || false;
    self.TASK_RESULT_DURABLE = undefined !== self.TASK_RESULT_DURABLE ? self.TASK_RESULT_DURABLE : true; // Set Durable true by default (Celery 3.1.7)
    self.ROUTES = self.ROUTES || {};

    self.broker_type = url.parse(self.BROKER_URL).protocol.slice(0, -1);
    if (self.broker_type === 'amqps')
        self.broker_type = 'amqp';
    debug('Broker type: ' + self.broker_type);
    checkProtocol('broker', self.broker_type);

    // backend
    self.RESULT_BACKEND = self.RESULT_BACKEND || self.BROKER_URL;

    self.backend_type = url.parse(self.RESULT_BACKEND).protocol.slice(0, -1);
    if (self.backend_type === 'amqps')
        self.backend_type = 'amqp';
    debug('Backend type: ' + self.backend_type);
    checkProtocol('backend', self.backend_type);
}

function RedisBroker(broker_url) {
    var self = this;
    var purl = url.parse(broker_url);
    var database;

    if (purl.pathname) {
      database = purl.pathname.slice(1);
    }

    self.redis = redis.createClient(purl.port || 6379,
                                    purl.hostname || 'localhost');

    if (purl.auth) {
        debug('Authenticating broker...');
        self.redis.auth(purl.auth.split(':')[1]);
        debug('Broker authenticated...');
    }

    if (database) {
        self.redis.select(database);
    }

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

function RedisBackend(conf) {
    var self = this;
    var purl = url.parse(conf.RESULT_BACKEND);
    var database;

    if (purl.pathname) {
      database = purl.pathname.slice(1);
    }

    debug('Connecting to backend...');
    self.redis = redis.createClient(purl.port, purl.hostname);
    // needed because we'll use `psubscribe`
    var backend_ex = self.redis.duplicate();

    if (purl.auth) {
        debug('Authenticating backend...');
        self.redis.auth(purl.auth.split(':')[1]);
        debug('Backend authenticated...');
    }

    if (database) {
        self.redis.select(database);
    }

    self.redis.on('error', function(err) {
        self.emit('error', err);
    });

    self.redis.on('end', function() {
        self.emit('end');
    });

    self.quit = function() {
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
        self.backend = amqp.createConnection({
            url: self.conf.BROKER_URL,
            heartbeat: 580
        }, {
            defaultExchangeName: self.conf.DEFAULT_EXCHANGE
        });
    } else if (self.conf.backend_type === self.conf.broker_type) {
        if (self.conf.backend_type === 'amqp') {
          self.backend = self.broker;
        }
    }

    // backend ready...
    self.backend.on('ready', function() {
        debug('Connecting to broker...');

        if (self.conf.broker_type === 'redis') {
            self.broker = new RedisBroker(self.conf.BROKER_URL);
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
    if (this.conf.backend_type === 'redis') {
        this.backend.quit();
    } else if (this.conf.broker_type !== this.conf.backend_type) {
        this.backend.quit();
    }
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

function Task(client, name, options, exchange) {
    var self = this;

    self.client = client;
    self.name = name;
    self.options = options || {};

    var route = self.client.conf.ROUTES[name],
        queue = route && route.queue;

    self.publish = function (args, kwargs, options, callback) {
        var id = options.id || uuid.v4();

        var result = new Result(id, self.client);

        if (client.conf.backend_type === 'redis') {
            client.backend.results[result.taskid] = result;
        }

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

        return result;
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

exports.createClient = function(config, callback) {
    return new Client(config, callback);
};

exports.createResult = function(taskId, client) {
    return new Result(taskId, client);
};
