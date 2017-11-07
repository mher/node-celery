var Result = require('./Result');
var uuid   = require('uuid');
var createMessage = require('../protocol').createMessage;

function Task(client, name, options, exchange) {
    var self = this;

    self.client = client;
    self.name = name;
    self.options = options || {};

    var route = self.client.conf.ROUTES[name],
        queue = route && route.queue;

    self.publish = function (args, kwargs, options, callback) {
        var id = options.id || uuid.v4();

        var result = new Result(id, self.client);

        if (client.conf.backend_type === 'redis') {
            client.backend.results[result.taskid] = result;
        }

        queue = options.queue || self.options.queue || queue || self.client.conf.DEFAULT_QUEUE;
        var msg = createMessage(self.name, args, kwargs, options, id);
        var pubOptions = {
            'contentType': 'application/json',
            'contentEncoding': 'utf-8'
        };

        if (exchange) {
            exchange.publish(queue, msg, pubOptions, callback);
        } else {
            self.client.broker.publish(queue, msg, pubOptions, callback);
        }

        return result;
    };
}

Task.prototype.call = function(args, kwargs, options, callback) {
    var self = this;

    args = args || [];
    kwargs = kwargs || {};
    options = options || self.options || {};

    if (!self.client.ready) {
        self.client.emit('error', 'Client is not ready');
    }
    else {
        return self.publish(args, kwargs, options, callback);
    }
};

module.exports = Task;
