var url = require('url'),
	util = require('util'),
	amqp = require('amqp'),
	redis = require('redis'),
	assert = require('assert'),
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
	self.DEFAULT_QUEUE = self.DEFAULT_QUEUE || 'celery';
	self.DEFAULT_EXCHANGE = self.DEFAULT_EXCHANGE || '';
	self.DEFAULT_EXCHANGE_TYPE = self.DEFAULT_EXCHANGE_TYPE || 'direct';
	self.DEFAULT_ROUTING_KEY = self.DEFAULT_ROUTING_KEY || 'celery';
	self.RESULT_EXCHANGE = self.RESULT_EXCHANGE || 'celeryresults';
	self.TASK_RESULT_EXPIRES = self.TASK_RESULT_EXPIRES || 86400000; // 1 day
	self.ROUTES = self.ROUTES || {};

	if (self.RESULT_BACKEND && self.RESULT_BACKEND.toLowerCase() === 'amqp') {
		self.backend_type = 'amqp';
	} else if (self.RESULT_BACKEND && url.parse(self.RESULT_BACKEND)
		.protocol === 'redis:') {
		self.backend_type = 'redis';
	}
}

function Client(conf, callback) {
	var self = this;

	self.conf = new Configuration(conf);

	self.ready = false;
	self.broker_connected = false;
	self.backend_connected = false;

	self.broker = amqp.createConnection({
		url: self.conf.BROKER_URL
	}, {
		defaultExchangeName: self.conf.DEFAULT_EXCHANGE
	}, callback);

	if (self.conf.backend_type === 'amqp') {
		self.backend = self.broker;
		self.backend_connected = true;
	} else if (self.conf.backend_type === 'redis') {
		var purl = url.parse(self.conf.RESULT_BACKEND),
			database = purl.pathname.slice(1);
		self.backend = redis.createClient(purl.port, purl.hostname);
		if (database) {
			self.backend.select(database);
		}

		var on_ready = function() {
			self.backend_connected = true;
			if (self.broker_connected) {
				self.ready = true;
				debug('Emiting connect event...');
				self.emit('connect');
			}
		};

		self.backend.on('connect', function() {
			debug('Backend connected...');
			if (purl.auth) {
				self.backend.auth(purl.auth.split(':')[1], on_ready);
			} else {
				on_ready();
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

	if (callback) {
		result.on('ready', callback);
	}
	return result;
};

function Task(client, name, options) {
	var self = this;

	self.client = client;
	self.name = name;
	self.options = options || {};

	var route = self.client.conf.ROUTES[name],
		queue = route && route.queue;

	self.publish = function(args, kwargs, options, callback) {
		var id = uuid.v4();
		self.client.broker.publish(
		self.options.queue || queue || self.client.conf.DEFAULT_QUEUE,
		createMessage(self.name, args, kwargs, options, id), {
			'contentType': 'application/json',
			'contentEncoding': 'utf-8'
		},
		callback);
		return new Result(id, self.client);
	};
}

Task.prototype.call = function(args, kwargs, options, callback) {
	var self = this;

	args = args || [];
	kwargs = kwargs || {};
	options = options || self.options || {};

	assert(self.client.ready);
	return self.publish(args, kwargs, options, callback);
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
			}
		},

		function(q) {
			q.bind(self.client.conf.RESULT_EXCHANGE, '#');
			q.subscribe(function(message) {
				self.result = message;
				//q.unbind('#');
				debug('Emiting ready event...');
				self.emit('ready', message);
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
		return self.result;
	}
};

exports.createClient = function(config, callback) {
	return new Client(config, callback);
};
