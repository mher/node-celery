# Celery client for Node.js

node-celery is a Celery client for Node. If you are new to Celery check out http://celeryproject.org/

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
	setTimout(function() {
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
	send_mail.call({
		to: 'to@example.com',
		title: 'hi'
	}); // sends a task to the mail queue
	calculate_rating.call({
		item: 1345
	}); // sends a task to the default queue
});
```
