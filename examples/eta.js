var celery = require('../celery'),
    client = celery.createClient({
        CELERY_BROKER_URL: 'amqp://guest:guest@localhost:5672//'
    });

client.on('error', function(err) {
    console.log(err);
});

client.on('connect', function() {
    client.call('tasks.send_email', {
        to: 'to@example.com',
        title: 'sample email'
    }, {
        eta: new Date(Date.now() + 60 * 60 * 1000) // an hour later
    });
});
