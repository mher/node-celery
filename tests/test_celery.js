var celery = require('../celery'),
    assert = require('assert');

var conf = {
    CELERY_BROKER_URL: 'amqp://',
    CELERY_RESULT_BACKEND: 'amqp'
};
var conf_redis = {
    CELERY_BROKER_URL: 'redis://'
};

describe('celery functional tests', function() {
    describe('initialization', function() {
        it('should create a client without error', function(done) {
            var client1 = celery.createClient(conf),
                client2 = celery.createClient({
                    CELERY_BROKER_URL: 'amqp://foo'
                });

            client1.on('connect', function() {
                client1.end();
            });

            client1.on('error', function(exception) {
                console.log(exception);
                assert.ok(false);
            });

            client1.once('end', function() {
                done();
            });

            client2.on('ready', function() {
                assert.ok(false);
            });

            client2.on('error', function(exception) {
                assert.ok(exception);
            });

            client2.once('end', function() {
                assert.ok(false);
            });
        });
    });

    describe('basic task calls', function() {
        it('should call a task without error', function(done) {
            var client = celery.createClient(conf),
                add = client.createTask('tasks.add');

            client.on('connect', function() {
                add.call([1, 2]);

                setTimeout(function() {
                    client.end();
                }, 100);
            });

            client.once('end', function() {
                done();
            });
        });

        it('should call a task without error', function(done) {
            var client = celery.createClient(conf_redis),
                add = client.createTask('tasks.add');

            client.on('connect', function() {
                add.call([1, 2]);

                setTimeout(function() {
                    client.end();
                }, 100);
            });

            client.once('end', function() {
                done();
            });
        });
    });

    describe('result handling with amqp backend', function() {
        it('should return a task result', function(done) {
            if (conf.CELERY_RESULT_BACKEND !== 'amqp') done();
            var client = celery.createClient(conf),
                add = client.createTask('tasks.add');

            client.on('connect', function() {
                var result = add.call([1, 2]);
                result.on('ready', function(message) {
                    assert.equal(message.result, 3);
                    client.end();
                });
            });

            client.on('end', function() {
                done();
            });
        });
    });

    describe('result handling with redis backend', function() {
        it('should return a task result', function(done) {
            if (conf.CELERY_RESULT_BACKEND === 'amqp') done();
            var client = celery.createClient(conf),
                add = client.createTask('tasks.add');

            client.on('connect', function() {
                var result = add.call([1, 2]);
                setTimeout(function() {
                    result.get(function(message) {
                        assert.equal(message.result, 3);
                        client.end();
                    });
                }, 1000);
            });

            client.on('end', function() {
                done();
            });
        });
    });

    describe('eta', function() {
        it('should call a task with a delay', function(done) {
            if (conf.CELERY_RESULT_BACKEND !== 'amqp') done();
            var client = celery.createClient(conf),
                time = client.createTask('tasks.time');

            client.on('connect', function() {
                var start = new Date()
                    .getTime(),
                    eta = new Date(start + 100000);
                var result = time.call(null, null, {
                    eta: eta
                });
                result.on('ready', function(message) {
                    //assert.ok(parseInt(message.result) - start > 1);
                    client.end();
                });
            });

            client.on('end', function() {
                done();
            });
        });
    });

    describe('expires', function() {
        it('should call a task which expires', function(done) {
            if (conf.CELERY_RESULT_BACKEND !== 'amqp') done();
            var client = celery.createClient(conf),
                time = client.createTask('tasks.time');

            client.on('connect', function() {
                var past = new Date(new Date()
                    .getTime() - 60 * 60 * 1000),
                    result = time.call(null, null, {
                        expires: past
                    });
                result.on('ready', function(message) {
                    assert.equal(message.status, 'REVOKED');
                    client.end();
                });
            });

            client.on('end', function() {
                done();
            });
        });
    });
});
