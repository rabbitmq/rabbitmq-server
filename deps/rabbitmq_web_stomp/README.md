# RabbitMQ Web STOMP plugin

This project is a simple bridge between the [RabbitMQ STOMP plugin](http://rabbitmq.com/stomp.html) and
WebSockets (directly or via SockJS emulation).

Once started the plugin opens a SockJS endpoint on prefix "/stomp" on
port 15674, for example a valid SockJS endpoint url may look like:
"http://127.0.0.1:15674/stomp".

Once the server is started you should be able to establish a SockJS
connection to this url. You will be able to communicate using the
usual STOMP protocol over it. For example, a page using Jeff Mesnil's
"stomp-websocket" project and SockJS may look like this:

    <script src="sockjs-0.3.min.js"></script>
    <script src="stomp.js"></script>
    <script>
        Stomp.WebSocketClass = SockJS;

        var client = Stomp.client('http://127.0.0.1:15674/stomp');
        var on_connect = function() {
            console.log('connected');
        };
        var on_error =  function() {
           console.log('error');
        };
        client.connect('guest', 'guest', on_connect, on_error, '/');
        [...]

See the RabbitMQ Web STOMP Examples plugin for more details.


## RabbitMQ Version Requirements

The most recent version of this plugin requires RabbitMQ `3.6.1` or later.

## Installation and Binary Builds

This plugin is now available from the [RabbitMQ community plugins page](http://www.rabbitmq.com/community-plugins.html).
Please consult the docs on [how to install RabbitMQ plugins](http://www.rabbitmq.com/plugins.html#installing-plugins).


## Building from Source

See [Plugin Development guide](http://www.rabbitmq.com/plugin-development.html).

TL;DR: running

    make dist

will build the plugin and put build artifacts under the `./plugins` directory.


## Copyright and License

(c) Pivotal Software Inc, 2007-20016

Released under the MPL, the same license as RabbitMQ.
