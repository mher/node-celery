echo 'Running protocol tests...'
mocha test_protocol

echo 'Running functional tests...'
mocha test_celery
