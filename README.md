# Celery client for Node.js

[![NPM Version](https://img.shields.io/npm/v/node-celery.svg)](https://img.shields.io/npm/v/node-celery.svg)
[![Downloads](https://img.shields.io/npm/dm/node-celery.svg)](https://img.shields.io/npm/dm/node-celery.svg)

Celery is an asynchronous task/job queue based on distributed
message passing. node-celery allows to queue tasks from Node.js.
If you are new to Celery check out http://celeryproject.org/

## Usage

Simple example, included as [examples/hello-world.js](https://github.com/mher/node-celery/blob/master/examples/hello-world.js):

```javascript
var celery = require('node-celery'),
	client = celery.createClient({
		CELERY_BROKER_URL: 'amqp://guest:guest@localhost:5672//',
		CELERY_RESULT_BACKEND: 'amqp'
	});

client.on('error', function(err) {
	console.log(err);
});

client.on('connect', function() {
	client.call('tasks.echo', ['Hello World!'], function(result) {
		console.log(result);
		client.end();
	});
});
```

**Note:** When using AMQP as result backend with celery prior to version
3.1.7 the result queue needs to be non durable or it will fail with a:
Queue.declare: (406) PRECONDITION_FAILED.

```javascript
var celery = require('node-celery'),
	client = celery.createClient({
		CELERY_TASK_RESULT_DURABLE: false
	});
```

For RabbitMQ backends, the entire broker options can be passed as an object that is handed off to AMQP.
This allows you to specify parameters such as SSL keyfiles, vhost, and connection timeout among others.

```javascript
var celery = require('node-celery'),
	client = celery.createClient({
		CELERY_BROKER_OPTIONS: {
			host: 'localhost',
			port: '5672',
			login: 'guest',
			password: 'guest',
			authMechanism: 'AMQPLAIN',
			vhost: '/',
			ssl: {
				enabled: true,
				keyFile: '/path/to/keyFile.pem',
				certFile: '/path/to/certFile.pem',
				caFile: '/path/to/caFile.pem'
			}
		},
		CELERY_RESULT_BACKEND: 'amqp'
	});
```

### ETA

The ETA (estimated time of arrival) lets you set a specific date and time that is the earliest time at which your task will be executed:

```javascript
var celery = require('node-celery'),
	client = celery.createClient({
		CELERY_BROKER_URL: 'amqp://guest:guest@localhost:5672//',
	});

client.on('connect', function() {
	client.call('send-email', {
		to: 'to@example.com',
		title: 'sample email'
	}, {
		eta: new Date(Date.now() + 60 * 60 * 1000) // an hour later
	});
});
```

### Expiration

The expires argument defines an optional expiry time, a specific date and time using Date:

```javascript
var celery = require('node-celery'),
	client = celery.createClient({
		CELERY_BROKER_URL: 'amqp://guest:guest@localhost:5672//',
	});

client.on('connect', function() {
	client.call('tasks.sleep', [2 * 60 * 60], null, {
		expires: new Date(Date.now() + 60 * 60 * 1000) // expires in an hour
	});
});
```

### Backends

The backend is used to store task results. Currently AMQP (RabbitMQ) and Redis backends are supported.

```javascript
var celery = require('node-celery'),
	client = celery.createClient({
		CELERY_BROKER_URL: 'amqp://guest:guest@localhost:5672//',
		CELERY_RESULT_BACKEND: 'redis://localhost/0'
	});

client.on('connect', function() {
	var result = client.call('tasks.add', [1, 2]);
	setTimeout(function() {
		result.get(function(data) {
			console.log(data); // data will be null if the task is not finished
		});
	}, 2000);
});
```

AMQP backend allows to subscribe to the task result and get it immediately, without polling:

```javascript
var celery = require('node-celery'),
	client = celery.createClient({
		CELERY_BROKER_URL: 'amqp://guest:guest@localhost:5672//',
		CELERY_RESULT_BACKEND: 'amqp'
	});

client.on('connect', function() {
	var result = client.call('tasks.add', [1, 2]);
	result.on('ready', function(data) {
		console.log(data);
	});
});
```

### Routing

The simplest way to route tasks to different queues is using CELERY_ROUTES configuration option:

```javascript
var celery = require('node-celery'),
	client = celery.createClient({
		CELERY_BROKER_URL: 'amqp://guest:guest@localhost:5672//',
		CELERY_ROUTES: {
			'tasks.send_mail': {
				queue: 'mail'
			}
		}
	}),
	send_mail = client.createTask('tasks.send_mail'),
	calculate_rating = client.createTask('tasks.calculate_rating');

client.on('error', function(err) {
	console.log(err);
});

client.on('connect', function() {
	send_mail.call([], {
		to: 'to@example.com',
		title: 'hi'
	}); // sends a task to the mail queue
	calculate_rating.call([], {
		item: 1345
	}); // sends a task to the default queue
});
```

