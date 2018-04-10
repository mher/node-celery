var celery = require('../celery'),
    assert = require('assert');

// $ docker-compose up -d

var conf_invalid = {
    CELERY_BROKER_URL: 'amqp://foo'
}

var conf_amqp = {
    CELERY_BROKER_URL: 'amqp://',
    CELERY_RESULT_BACKEND: 'amqp'
};

var conf_redis = {
    CELERY_BROKER_URL: 'redis://',
    CELERY_RESULT_BACKEND: 'redis://',
    TASK_RESULT_EXPIRES: 1 // seconds
};

var conf_sentinel = {
    CELERY_BROKER_URL: 'sentinel://',
    CELERY_RESULT_BACKEND: 'sentinel://',
    TASK_RESULT_EXPIRES: 1, // seconds,
    CELERY_BROKER_OPTIONS: {
      sentinels: [{ host: '127.0.0.1', port: 26379 }],
      name: 'sentinelTest'
    },
    RESULT_BACKEND_OPTIONS: {
      sentinels: [{ host: '127.0.0.1', port: 26379 }],
      name: 'sentinelTest'
    }
};

describe('celery functional tests', function() {
    describe('initialization', function() {
        it('should create a valid amqp client without error', function(done) {
            var client = celery.createClient(conf_amqp);

            assert.equal(client.conf.BROKER_OPTIONS.url, 'amqp://');
            assert.equal(client.conf.BROKER_OPTIONS.heartbeat, 580);
            assert.equal(client.conf.broker_type, 'amqp');

            assert.equal(client.conf.RESULT_BACKEND_OPTIONS.url, 'amqp://');
            assert.equal(client.conf.RESULT_BACKEND_OPTIONS.heartbeat, 580);
            assert.equal(client.conf.backend_type, 'amqp');

            client.on('connect', function() {
                client.end();
            });

            client.on('error', function(exception) {
                assert.ok(false);
            });

            client.once('end', function() {
                done();
            });
        });

        it('should create a valid redis client without error', function(done) {
            var client = celery.createClient(conf_redis);

            assert.equal(client.conf.BROKER_OPTIONS.url, 'redis://');
            assert.equal(client.conf.broker_type, 'redis');

            assert.equal(client.conf.RESULT_BACKEND_OPTIONS.url, 'redis://');
            assert.equal(client.conf.backend_type, 'redis');

            client.on('connect', function() {
                client.end();
            });

            client.on('error', function(exception) {
                assert.ok(false);
            });

            client.once('end', function() {
                done();
            });
        });

        it('should throw error on invalid amqp client', function(done) {
            var client = celery.createClient(conf_invalid);

            client.on('ready', function() {
                assert.ok(false);
            });

            client.on('error', function(exception) {
                assert.ok(exception);
                client.emit('end');
            });

            client.once('end', function() {
                done();
            });
        });
    });

    describe('basic task calls', function() {
        it('should call a task without error', function(done) {
            var client = celery.createClient(conf_amqp),
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
            var client = celery.createClient(conf_amqp),
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
        it('should return a task result (poll)', function(done) {
            var client = celery.createClient(conf_redis),
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

        it('should return a task result (push)', function(done) {
            var client = celery.createClient(conf_redis),
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

    describe('result handling with redis sentinel backend', function() {
        it('should return a task result (poll)', function(done) {
            var client = celery.createClient(conf_sentinel),
                add = client.createTask('tasks.add');

            client.on('connect', function() {
                var result = add.call([1, 2]);

                setTimeout(function() {
                    result.get(function(message) {
                        assert.equal(message.result, 3);
                        client.end();
                    });
                }, 500);
            });

            client.on('end', function() {
                done();
            });
        });

        it('should return a task result (push)', function(done) {
            var client = celery.createClient(conf_sentinel),
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

    describe('eta', function() {
        it('should call a task with a delay', function(done) {
            var client = celery.createClient(conf_amqp),
                time = client.createTask('tasks.time');

            client.on('connect', function() {
                var start = new Date().getTime(),
                    eta = new Date(start + 1000);
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
            var client = celery.createClient(conf_amqp),
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
