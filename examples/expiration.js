var celery = require('../celery'),
    client = celery.createClient({
        CELERY_BROKER_URL: 'amqp://guest:guest@localhost:5672//'
    });

client.on('error', function(err) {
    console.log(err);
});

client.on('connect', function() {
    client.call('tasks.sleep', [2 * 60 * 60], null, {
        expires: new Date(Date.now() + 60 * 60 * 1000) // expires in an hour
    });
});
