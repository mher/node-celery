process.env.NODE_CELERY_DEBUG = 0;

var celery = require('../celery'),
	util = require('util');

var client = celery.createClient({
	CELERY_BROKER_URL: 'amqp://',
	CELERY_RESULT_BACKEND: 'amqp'
});

client.on('error', function(err) {
	console.log(err);
});

var n = parseInt(process.argv.length > 2 ? process.argv[2] : 1000, 10);

client.once('connect', function() {
	var start = Date.now();
	var j = n;
	onresult = function(result) {
		j--;
		if (j === 1) {
			console.log(util.format('Execued %d tasks in %s milliseconds',
			n, Date.now() - start));
			client.end();
		}
	};
	for (var i = 0; i < n; i++) {
		client.call('tasks.add', [i, i], null, null, onresult);
	}
});
