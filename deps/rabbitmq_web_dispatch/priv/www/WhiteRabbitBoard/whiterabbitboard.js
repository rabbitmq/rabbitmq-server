function genguid() {
    return "g" + Math.floor(Math.random() * 1e10);
}

var sendCanvasPainterEvent;
var listenToCanvasPainterEvents;
var rabbitReadyCallback = null;

function setupWhiteRabbitBoard() {
    log("Starting.");

    var channel;
    var queueName;
    var exchangeName = "canvasPainter";

    openRabbitChannel(on_open, { debug: true, debugLogger: log });

    function on_open(c) {
	channel = c;
	log("on_open");
	channel.exchangeDeclare(exchangeName, "fanout")
	.addCallback(on_exchange_declared);
    }

    function on_exchange_declared() {
	log("on_exchange_declared");
	channel.queueDeclare().addCallback(on_queue_declared);
    }

    function on_queue_declared(newQueueName) {
	log("on_queue_declared");
	queueName = newQueueName;
	channel.queueBind(queueName, exchangeName).addCallback(on_queue_bound);
    }

    function on_queue_bound() {
	log("on_queue_bound");
	sendCanvasPainterEvent = function (event) {
	    channel.basicPublish(exchangeName, "", JSON.stringify(event));
	};
	listenToCanvasPainterEvents = function (callback) {
	    channel.basicConsume(queueName,
				 {
				     consumeOk: function(tag) {
					 log({consumeOk: tag});
				     },
				     deliver: function(delivery) {
					 //log({delivery: delivery});
					 var parsedMessage = JSON.parse(delivery.content);
					 //log({parsedMessage: parsedMessage});
					 callback(parsedMessage);
				     }
				 },
				 { no_ack: true });
	};
	rabbitReadyCallback();
    }
}

function log() {
    $A(arguments).each(function (arg) {
			   if (typeof(arg) == 'string') {
			       $("testOutput").appendChild(document.createTextNode(arg + "\n"));
			   } else {
			       $("testOutput").appendChild(document
							   .createTextNode(JSON.stringify(arg) +
									   "\n"));
			   }
		       });
}
