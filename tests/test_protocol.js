var assert = require('assert'),
    uuid = require('node-uuid'),
    createMessage = require('../protocol')
        .createMessage;


function msg(task, args, kwargs, options, id) {
    id = id || 'id';
    return JSON.parse(createMessage(task, args, kwargs, options, id));
}

describe('protocol', function() {
    describe('#createMessage', function() {
        it('should create a message with default args', function() {
            assert.deepEqual(msg("foo"), {
                task: "foo",
                args: [],
                kwargs: {},
                id: "id"
            });
        });

        it('should create a message with the given args', function() {
            assert.deepEqual(msg("foo", [1, 2]), {
                task: "foo",
                args: [1, 2],
                kwargs: {},
                id: "id"
            });
            assert.deepEqual(msg("foo", null, {
                bar: 3
            }), {
                task: "foo",
                args: [],
                kwargs: {
                    bar: 3
                },
                id: "id"
            });
            assert.deepEqual(msg("foo", null, null, null, "bar"), {
                task: "foo",
                args: [],
                kwargs: {},
                id: "bar"
            });
        });

        it('should send the expiry as UTC', function() {
            assert.deepEqual(msg("foo", null, null, {
                expires: Date.parse('Mon Nov 30 2015 10:03:37 GMT+0000 (UTC)')
            }), {
                task: "foo",
                args: [],
                kwargs: {},
                expires: "2015-11-30T10:03:37.000Z",
                id: "id"
            });
        });
    });
});
