
RabbitMQ-Web-Stomp plugin
=========================

This project is a simple bridge between "RabbitMQ-stomp" plugin and
SockJS.

Once started the plugin opens a SockJS endpoint on prefix "/stomp" on
port 55674, for example a valid SockJS endpoint url may look like:
"http://127.0.0.1:55674/stomp".

Once the server is started you should be able to establish a SockJS
connection to this url. You will be able to communicate using the
usual STOMP protocol over it. For example, a page using Jeff Mensnil's
"stomp-websocket" project may look like this:


    <script src="http://cdn.sockjs.org/sockjs-0.3.min.js"></script>
    <script src="stomp.js"></script>
    <script>
        WebSocketStompMock = SockJS;

        var client = Stomp.client('http://127.0.0.1:55674/stomp');
        [...]

See the "RabbitMQ-Web-Stomp-examples" plugin for more details.


Installation
------------

Instructions on how to install a plugin into RabbitMQ broker:

  * http://www.rabbitmq.com/plugins.html#installing-plugins

You'll need few dependencies:

 * rabbitmq-web-stomp
 * cowboy_wrapper
 * sockjs_erlang_wrapper
 * rabbitmq_stomp
 * amqp_client
