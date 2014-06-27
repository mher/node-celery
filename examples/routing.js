var celery = require('../celery'),
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
