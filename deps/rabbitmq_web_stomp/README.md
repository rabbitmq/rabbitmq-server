RabbitMQ-Web-Stomp plugin
=========================

This project is a simple bridge between "RabbitMQ-stomp" plugin and
SockJS.

Once started the plugin opens a SockJS endpoint on prefix "/stomp" on
port 15674, for example a valid SockJS endpoint url may look like:
"http://127.0.0.1:15674/stomp".

Once the server is started you should be able to establish a SockJS
connection to this url. You will be able to communicate using the
usual STOMP protocol over it. For example, a page using Jeff Mesnil's
"stomp-websocket" project may look like this:


    <script src="http://cdn.sockjs.org/sockjs-0.3.min.js"></script>
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

See the "RabbitMQ-Web-Stomp-examples" plugin for more details.


Installation
------------

Generic build instructions are at:

 * http://www.rabbitmq.com/plugin-development.html

Instructions on how to install a plugin into RabbitMQ broker:

  * http://www.rabbitmq.com/plugins.html#installing-plugins

