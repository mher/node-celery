process.env.NODE_CELERY_DEBUG = 0;

var celery = require('../celery'),
	util = require('util');

var client = celery.createClient({
	CELERY_BROKER_URL: 'amqp://'
});

client.on('error', function(err) {
	console.log(err);
});

var n = parseInt(process.argv.length > 2 ? process.argv[2] : 1000, 10);

client.once('connect', function() {
	var start = Date.now();
	for (var i = 0; i < n; i++) {
		client.call('tasks.add', [i, i]);
	}

	console.log(util.format('Published %d messages in %s milliseconds',
	n, Date.now() - start));
});
