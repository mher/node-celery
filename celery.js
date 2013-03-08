var uuid = require('node-uuid'),
    url = require('url'),
    util = require('util'),
    amqp = require('amqp'),
    events = require('events');

var createMessage = require('./protocol').createMessage;

function Client(conf) {
    this.conf = conf || {};
    this.connected = false;

    this.connection = amqp.createConnection(
            {url: this.conf.broker || 'amqp://'});

    var that = this;

    this.connection.on('ready', function () {
        that.connected = true;
        that.emit('connect');
    });

    this.connection.on('error', function (exception) {
        that.emit('error', exception);
    });

    this.connection.on('end', function () {
        that.emit('end');
    });

    this.default_queue = this.conf.CELERY_DEFAULT_QUEUE || 'celery';
    this.result_exchange = this.conf.CELERY_RESULT_EXCHANGE || 'celeryresults';
    this.task_result_expires = this.conf.CELERY_TASK_RESULT_EXPIRES || 86400000; // 1day
    this.result_backend = 'amqp';
}

util.inherits(Client, events.EventEmitter);

Client.prototype.createTask = function (name, options) {
    return new Task(this, name, options);
};

Client.prototype.end = function () {
    this.connection.end();
};

function Task(client, name, options) {
    this.client = client;
    this.name = name;
    this.options = options || {};

    var that = this;

    this.publish = function (args, kwargs, options, callback) {
        var id = uuid.v4();
        that.client.connection.publish(
                options.queue || that.client.default_queue,
                createMessage(that.name, args, kwargs, options, id),
                {'contentType': 'application/json'},
                callback);
        return new Result(id, that.client);
    };
}

Task.prototype.call = function(args, kwargs, options, callback) {
    var that = this;

    args = args || [];
    kwargs = kwargs || {};

    if (this.client.connected) {
        return this.publish(args, kwargs, options, callback);
    }
    else {
        this.client.once('connect', function () {
            that.publish(args, kwargs, options, callback);
        });
    }
};

function Result(taskid, client) {
    events.EventEmitter.call(this);
    this.taskid = taskid;
    this.client = client;
    this.result = null;

    var that = this;

    switch (this.client.result_backend) {
        case 'amqp': {
            this.client.connection.queue(
                this.taskid.replace(/-/g, ''),
                {"arguments": {'x-expires':this.client.task_result_expires}},
                function(q) {
                    q.bind(that.client.result_exchange, '#');
                    q.subscribe(function (message) {
                        that.result = message;
                        q.unbind('#');
                        that.emit("result", message);
                    });
            });
            break;
        }
    }
}

util.inherits(Result, events.EventEmitter);

exports.createClient = function (config) {
    return new Client(config);
};
