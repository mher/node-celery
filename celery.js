var uuid = require('node-uuid'),
	url = require('url'),
	util = require('util'),
	amqp = require('amqp'),
	redis = require('redis'),
	events = require('events');

var createMessage = require('./protocol').createMessage;


function Client(conf) {
	var self = this;

	this.conf = conf || {};
	this.ready = false;
	this.broker_connected = false;
	this.backend_connected = false;

	this.broker = amqp.createConnection({
		url: this.conf.broker || 'amqp://'
	});

	if (this.conf.result_backend && this.conf.result_backend.toLowerCase() === 'amqp') {
		this.backend = this.broker;
		this.backend_connected = true;
	} else if (this.conf.result_backend && url.parse(this.conf.result_backend)
		.protocol === 'redis:') {
		this.backend = redis.createClient();

		this.backend.on('connect', function() {
			self.backend_connected = true;
			if (self.broker_connected) {
				self.ready = true;
				self.emit('connect');
			}
		});

		this.backend.on('error', function(err) {
			self.emit('error', err);
		});
	}

	this.broker.on('ready', function() {
		self.broker_connected = true;
		if (self.backend_connected) {
			self.ready = true;
			self.emit('connect');
		}
	});

	this.broker.on('error', function(err) {
		self.emit('error', err);
	});

	this.broker.on('end', function() {
		self.emit('end');
	});

	this.default_queue = this.conf.CELERY_DEFAULT_QUEUE || 'celery';
	this.result_exchange = this.conf.CELERY_RESULT_EXCHANGE || 'celeryresults';
	this.task_result_expires = this.conf.CELERY_TASK_RESULT_EXPIRES || 86400000; // 1day
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
	this.client = client;
	this.name = name;
	this.options = options || {};

	var self = this;

	this.publish = function(args, kwargs, options, callback) {
		options = options || {};
		var id = uuid.v4();
		self.client.broker.publish(
		options.queue || self.client.default_queue,
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
	events.EventEmitter.call(this);
	this.taskid = taskid;
	this.client = client;
	this.result = null;

	var self = this;

	if (this.client.result_backend && this.client.result_backend.toLowerCase() === 'amqp') {
		this.client.backend.queue(
		this.taskid.replace(/-/g, ''), {
			"arguments": {
				'x-expires': this.client.task_result_expires
			}
		},

		function(q) {
			q.bind(self.client.result_exchange, '#');
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
