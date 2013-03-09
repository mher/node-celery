var uuid = require('node-uuid'),
	url = require('url'),
	util = require('util'),
	amqp = require('amqp'),
	redis = require('redis'),
	events = require('events');

var createMessage = require('./protocol').createMessage;

function Configuration(options) {
    var self = this;

	for (var o in options) {
		if (options.hasOwnProperty(o)) {
			self[o] = options[o].replace(/^CELERY_/, '');
		}
	}

    self.BROKER = self.BROKER || 'amqp://';
	self.DEFAULT_QUEUE = self.DEFAULT_QUEUE || 'celery';
	self.RESULT_EXCHANGE = self.RESULT_EXCHANGE || 'celeryresults';
	self.TASK_RESULT_EXPIRES = self.TASK_RESULT_EXPIRES || 86400000; // 1 day

    if (self.RESULT_BACKEND && self.RESULT_BACKEND.toLowerCase() === 'amqp') {
        self.backend_type = 'amqp';
    }
	else if (self.RESULT_BACKEND && url.parse(self.RESULT_BACKEND).protocol === 'redis:') {
        self.backend_type = 'redis';
    }
}

function Client(conf) {
	var self = this;

	self.conf = new Configuration(conf);
	self.ready = false;
	self.broker_connected = false;
	self.backend_connected = false;

	self.broker = amqp.createConnection({
		url: self.conf.BROKER,
	});

	if (self.conf.backend_type === 'amqp') {
		self.backend = self.broker;
		self.backend_connected = true;
	} else if (self.conf.backend_type === 'redis') {
		self.backend = redis.createClient();

		self.backend.on('connect', function() {
			self.backend_connected = true;
			if (self.broker_connected) {
				self.ready = true;
				self.emit('connect');
			}
		});

		self.backend.on('error', function(err) {
			self.emit('error', err);
		});
	}

	self.broker.on('ready', function() {
		self.broker_connected = true;
		if (self.backend_connected) {
			self.ready = true;
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

Client.prototype.createTask = function(name, options) {
	return new Task(this, name, options);
};

Client.prototype.end = function() {
	this.broker.end();
	if (this.broker !== this.backend) {
		this.backend.end();
	}
};

function Task(client, name, options) {
	var self = this;

	self.client = client;
	self.name = name;
	self.options = options || {};

	self.publish = function(args, kwargs, options, callback) {
		options = options || {};
		var id = uuid.v4();
		self.client.broker.publish(
		options.queue || self.client.conf.DEFAULT_QUEUE,
		createMessage(self.name, args, kwargs, options, id), {
			'contentType': 'application/json'
		},
		callback);
		return new Result(id, self.client);
	};
}

Task.prototype.call = function(args, kwargs, options, callback) {
	var self = this;

	args = args || [];
	kwargs = kwargs || {};

	if (this.client.broker_connected) {
		return this.publish(args, kwargs, options, callback);
	} else {
		this.client.broker.once('connect', function() {
			self.publish(args, kwargs, options, callback);
		});
	}
};

function Result(taskid, client) {
	var self = this;

	events.EventEmitter.call(self);
	self.taskid = taskid;
	self.client = client;
	self.result = null;


	if (self.client.conf.backend_type === 'amqp') {
		self.client.backend.queue(
		self.taskid.replace(/-/g, ''), {
			"arguments": {
				'x-expires': self.client.conf.TASK_RESULT_EXPIRES
			}
		},

		function(q) {
			q.bind(self.client.conf.RESULT_EXCHANGE, '#');
			q.subscribe(function(message) {
				self.result = message;
				//q.unbind('#');
				self.emit("result", message);
			});
		});
	}
}

util.inherits(Result, events.EventEmitter);

Result.prototype.get = function(callback) {
	var self = this;
	if (callback && self.result == null) {
		this.client.backend.get('celery-task-meta-' + self.taskid, function(err, reply) {
			self.result = JSON.parse(reply);
			callback(self.result);
		});
	} else {
		return self.result;
	}
};

exports.createClient = function(config) {
	return new Client(config);
};
