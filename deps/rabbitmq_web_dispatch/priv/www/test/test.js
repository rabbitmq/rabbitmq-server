function testMain() {
    log("test_main");

    var channelFactory = new JsonRpcService("/rpc/rabbitmq", handle_service_ready,
					    {debug: true,
					     debugLogger: log,
					     timeout: 30000});
    var channel;

    // service gateway ready
    function handle_service_ready() {
        log("open");
	channel = new RabbitChannel(channelFactory, handle_channel_ready,
				    { debug: true,
				      debugLogger: log,
				      channelTimeout: 5 });
     }

    function handle_channel_ready() {
        log("handle_channel_ready");

	var queue1 = "test-queue-1a";
	var queue2 = "test-queue-1b";

	var tag1 = "aa-cons-tag1";

	var msg1 = "hello, world";
	var msg2 = "hello, world, again! pub 2";

	channel.queueDeclare(queue1)
	.addCallback(function(reply)
	{
	    log({q1: reply});
	    channel.basicConsume(queue1,
				 {
				     consumeOk: function(tag) {
					 log({consumeOk: tag});
					 this.tag = tag;
				     },
				     deliver: function(delivery) {
					 log({delivery: delivery});
					 channel.basicAck(delivery.delivery_tag);
					 channel.basicCancel(this.tag);
				     },
				     cancelOk: function(tag) {
					 log({cancelOk: tag});
				     }
				 },
				 { consumer_tag: tag1 })
	    .addCallback(function () {
			     channel.basicPublish("", queue1, msg1);
			 });
	});

        channel.queueDeclare(queue2)
        .addCallback(function(reply)
	{
            log({q2: reply});
	    channel.basicConsume(queue2,
				 {
				     consumeOk: function(tag) {
					 this.tag = tag;
				     },
				     deliver: function(delivery) {
					 log({delivery2: delivery});
					 channel.basicAck(delivery.delivery_tag);
					 channel.basicCancel(this.tag)
					 .addCallback(reopen);
				     }
				 })
	    .addCallback(function () {
			     channel.basicPublish("", queue2, msg2,
						  {reply_to: "something22"});
			 });
        });
    }

    function reopen() {
        channel.close();
        channel = new RabbitChannel(channelFactory, test_cancel,
				    { debug: true,
				      debugLogger: log,
				      channelTimeout: 6 });
    }

    function test_cancel(channel) {
        log("test basic.cancel compliance");

        var queue = "test-queue-4";
        var ctag = "my-consumer";

	channel.queueDeclare(queue, false, false, true)
	.addCallback(function ()
	{
	    log("queue declare OK");
	    channel.basicConsume(queue,
				 {
				     deliver: function(delivery) {
					 log({delivery4: delivery});
					 channel.basicCancel("this-never-existed")
					 .addCallback(function (x) {
							  log({"never existed": x});
						      });
					 channel.basicCancel(ctag)
					 .addCallback(function (x) {
							  log({cancelled: x});
							  channel.basicPublish("", queue, "Two");
						      });
				     }
				 },
				 { consumer_tag: ctag,
				   no_ack: true })
	    .addCallback(function () {
			     channel.basicPublish("", queue, "One");
			 });
	});
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
