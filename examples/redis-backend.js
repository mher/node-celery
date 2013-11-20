var celery = require('../celery'),
	client = celery.createClient({
		CELERY_BROKER_URL: 'amqp://guest:guest@localhost:5672//',
		CELERY_RESULT_BACKEND: 'redis://localhost:6379'
	});

client.on('error', function(err) {
	console.log(err);
});

client.on('connect', function() {
	var result = client.call('tasks.add', [1, 2]);
	setTimout(function() {
		result.get(function(data) {
			console.log(data); // data will be null if the task is not finished
		});
	}, 2000);
});
