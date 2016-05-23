var uuid = require('node-uuid');

var fields = ['task', 'id', 'args', 'kwargs', 'retries', 'eta', 'expires', 'queue',
              'taskset', 'chord', 'utc', 'callbacks', 'errbacks', 'timeouts'];


function formatDate(date) {
    return new Date(date).toISOString();
}

function createMessage(task, args, kwargs, options, id) {
    args = args || [];
    kwargs = kwargs || {};

    var message = {
        task: task,
        args: args,
        kwargs: kwargs
    };

    message.id = id || uuid.v4();
    for (var o in options) {
        if (options.hasOwnProperty(o)) {
            if (fields.indexOf(o) === -1) {
                throw "invalid option: " + o;
            }
            message[o] = options[o];
        }
    }

    if (message.eta) {
        message.eta = formatDate(message.eta);
    }

    if (message.expires) {
        message.expires = formatDate(message.expires);
    }

    return JSON.stringify(message);
}

exports.createMessage = createMessage;
