var assert = require('assert'),
    uuid = require('node-uuid'),
    createMessage = require('../protocol').createMessage;


uuid.v4 = function() {
    return "id";
}

function msg (task, args, kwargs, options, id) {
    return JSON.parse(createMessage(task, args, kwargs, options, id));
}


describe('protocol', function(){
  describe('#createMessage', function(){
    it('should create a message with default args', function(){
        assert.deepEqual(msg("foo"), {task: "foo", args: [], kwargs: {}, id: "id"});
    })

    it('should create a message with the given args', function(){
        assert.deepEqual(msg("foo", [1, 2]), {task: "foo", args: [1, 2], kwargs: {}, id: "id"});
        assert.deepEqual(msg("foo", null, {bar: 3}), {task: "foo", args: [], kwargs: {bar: 3}, id: "id"});
        assert.deepEqual(msg("foo", null, null, null, "bar"), {task: "foo", args: [], kwargs: {}, id: "bar"});
    })
  })
})

// add a test for invalid options
