//   The contents of this file are subject to the Mozilla Public License
//   Version 1.1 (the "License"); you may not use this file except in
//   compliance with the License. You may obtain a copy of the License at
//   http://www.mozilla.org/MPL/
//
//   Software distributed under the License is distributed on an "AS IS"
//   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
//   License for the specific language governing rights and limitations
//   under the License.
//
//   The Original Code is RabbitMQ.
//
//   The Initial Developers of the Original Code are LShift Ltd,
//   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
//
//   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
//   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
//   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
//   Technologies LLC, and Rabbit Technologies Ltd.
//
//   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
//   Ltd. Portions created by Cohesive Financial Technologies LLC are
//   Copyright (C) 2007-2009 Cohesive Financial Technologies
//   LLC. Portions created by Rabbit Technologies Ltd are Copyright
//   (C) 2007-2009 Rabbit Technologies Ltd.
//
//   All Rights Reserved.
//
//   Contributor(s): ______________________________________.
//
//

function openRabbitChannel(readyFn, options) {
    var o = {
	factoryServiceUrl: "/rpc/rabbitmq",
	timeout: 30000 // timeout for the *factory*, not the channel
    };
    Object.extend(o, options || {});

    var factoryService = new JsonRpcService(o.factoryServiceUrl, onServiceReady, o);
    function onServiceReady() {
	new RabbitChannel(factoryService, readyFn, o);
    }
}

RabbitChannel = Class.create();
Object.extend(RabbitChannel.prototype,
{
    initialize: function(factory, readyFn, options) {
	this.options = {
	    rpcServiceUrlBase: "/rpc/",
	    username: "guest",
	    password: "guest",
	    virtualHost: null,
	    realm: "/data",
	    debug: false,
	    debugLogger: alert,
	    channelTimeout: 10 /* seconds; zero means "do not specify" */
	};
	Object.extend(this.options, options || {});
        this.consumers = {};
        this.alive = true;
	this.ticket = null;

	factory.open(this.options.username,
		     this.options.password,
		     this.options.channelTimeout,
		     this.options.virtualHost)
	.addCallback(channel_created.bind(this));

	function channel_created(reply) {
	    this.service = new JsonRpcService(this.options.rpcServiceUrlBase + reply.service,
					      ready.bind(this),
					      {debug: this.options.debug,
					       debugLogger: this.options.debugLogger,
					       timeout: this.options.channelTimeout * 1000});
	}

	function ready(result) {
            this.poll_tophalf();
            Event.observe(window, 'unload', this.close.bind(this));
            Event.observe(window, 'pagehide', this.close.bind(this));

	    this.accessRequest(this.options.realm)
	    .addCallback(ticket_request_complete.bind(this));
	}

	function ticket_request_complete() {
            readyFn(this);
        }
    },

    _dval: function(v, d) {
	return (v == null) ? d : v;
    },

    _call: function(method, args) {
	if (this.alive) {
	    return this.service.call(method, args);
	}
    },

    _cast: function(method, args, content, props) {
	if (this.alive) {
	    this.service.cast(method, args, content, props)
	    .addCallback(this.handlePollResult.bind(this));
	}
    },

    _extractArg: function(index) {
	return function(reply) { return reply.args[index]; };
    },

    _setTicket: function(ticket) {
	this.ticket = ticket;
    },

    accessRequest: function(realm, exclusive, passive, active, write, read) {
	return this._call("access.request", [this._dval(realm, "/data"),
					     this._dval(exclusive, false),
					     this._dval(passive, true),
					     this._dval(active, true),
					     this._dval(write, true),
					     this._dval(read, true)])
	.addReplyTransformer(this._extractArg(0))
	.addCallback(this._setTicket.bind(this));
    },

    exchangeDeclare: function(exchange, type, passive, durable, auto_delete, arguments) {
        return this._call("exchange.declare", [this.ticket,
					       exchange,
					       this._dval(type, "direct"),
					       this._dval(passive, false),
					       this._dval(durable, false),
					       this._dval(auto_delete, false),
					       false, // internal
					       false, // nowait
					       this._dval(arguments, {})]);
    },

    queueDeclare: function(queue, passive, durable, exclusive, auto_delete, arguments) {
        return this._call("queue.declare", [this.ticket,
					    this._dval(queue, ""),
					    this._dval(passive, false),
					    this._dval(durable, false),
					    this._dval(exclusive, false),
					    this._dval(auto_delete, true),
					    false, // nowait
					    this._dval(arguments, {})])
	.addReplyTransformer(this._extractArg(0));
    },

    queueDelete: function(queue, if_unused, if_empty) {
        return this._call("queue.delete", [this.ticket,
					   this._dval(queue, ""),
					   this._dval(if_unused, false),
					   this._dval(if_empty, false),
					   false // nowait
					  ])
	.addReplyTransformer(this._extractArg(0));
    },

    queueBind: function(queue, exchange, routing_key, arguments) {
        return this._call("queue.bind", [this.ticket,
					 queue,
					 exchange,
					 this._dval(routing_key, ""),
					 false, // nowait
					 this._dval(arguments, {})]);
    },

    basicConsume: function(queue, consumer, options) {
	o = {
	    consumer_tag: "",
	    no_local: false,
	    no_ack: false,
	    exclusive: false
	};
	Object.extend(o, options || {});
	return this._call("basic.consume", [this.ticket,
					    queue,
					    o.consumer_tag,
					    o.no_local,
					    o.no_ack,
					    o.exclusive,
					    false // nowait
					   ])
	.addReplyTransformer(this._extractArg(0))
	.addCallback((function (tag) {
			  this.consumers[tag] = consumer;
			  if (consumer.consumeOk) {
			      consumer.consumeOk(tag);
			  }
		      }).bind(this));
    },

    _js_props: function(props) {
	return { content_type: props[0],
		 content_encoding: props[1],
		 headers: props[2],
		 delivery_mode: props[3],
		 priority: props[4],
		 correlation_id: props[5],
		 reply_to: props[6],
		 expiration: props[7],
		 message_id: props[8],
		 timestamp: props[9],
		 type: props[10],
		 user_id: props[11],
		 app_id: props[12],
		 cluster_id: props[13] };
    },

    _amqp_props: function(props) {
	return [props.content_type,
		props.content_encoding,
		props.headers,
		props.delivery_mode,
		props.priority,
		props.correlation_id,
		props.reply_to,
		props.expiration,
		props.message_id,
		props.timestamp,
		props.type,
		props.user_id,
		props.app_id,
		props.cluster_id];
    },

    basicPublish: function(exchange, routing_key, message, props, mandatory, immediate) {
	this._cast("basic.publish", [this.ticket,
				     exchange,
				     routing_key,
				     this._dval(mandatory, false),
				     this._dval(immediate, false)],
		   message, this._amqp_props(props || {}));
    },

    basicAck: function(delivery_tag, multiple) {
	this._cast("basic.ack", [delivery_tag,
				 this._dval(multiple, false)]);
    },

    basicCancel: function(consumer_tag) {
	return this._call("basic.cancel", [consumer_tag,
					   false // nowait
					  ])
	.addReplyTransformer(this._extractArg(0))
	.addCallback((function (tag) {
			  var consumer = this.consumers[tag];
			  delete this.consumers[tag];
			  if (consumer.cancelOk) {
			      consumer.cancelOk(tag);
			  }
		      }).bind(this));
    },

/*
    queue_bind: function(o) {
        return this.service.call(new QueueBind().update(o));
    },

    queue_delete : function(o) {
        return this.service.call(new QueueDelete().update(o));
    },

    exchange_declare: function(o) {
        return this.service.call(new ExchangeDeclare().update(o));
    },

    exchange_delete : function() {
        //TODO
    },

    basic_get: function(o) {
        return this.service.call(new BasicGet().update(o));
    },
*/

    poll_tophalf: function() {
        if (this.alive) {
            this.service.poll()
	    .addCallback(this.handlePollResult.bind(this))
	    .addCallback(this.poll_tophalf.bind(this));
	}
    },

    close: function() {
        if (this.alive) {
            this.alive = false;
            this.service.close()
	    .addCallback(this.handlePollResult.bind(this));
        }
    },

    handlePollResult: function(result) {
        result.each(this.handleAsyncMessage.bind(this));
    },

    handleAsyncMessage: function (message) {
	var handler = this["handle_async_" + message.method];
	if (handler) {
	    (handler.bind(this))(message.args,
				 message.content,
				 message.props);
	} else {
            if (this.options.debug) {
		this.options.debugLogger({async: message});
	    }
	}
    },

    "handle_async_basic.deliver": function(args, content, props) {
        var consumer = this.consumers[args[0]];
        if (consumer) {
	    try {
		consumer.deliver({content: content,
				  delivery_tag: args[1],
				  redelivered: args[2],
				  exchange: args[3],
				  routing_key: args[4],
				  props: this._js_props(props)});
	    } catch (err) {}
        }
    }
});

AmqpRpcClient = Class.create();
Object.extend(AmqpRpcClient.prototype, {
    initialize: function(service, exchange, routing_key) {
        log(">>>>>>>>>>>>>>> ampq rpc client initing");
        this.amqp = service;
        this.exchange = exchange;
        this.routing_key = routing_key || "";
        this.nextCorrelationId = 333;
        this.transactionMap = {};
        this.queueName = null;
        this.pendingRequests = [];

        service.queueDeclare("", false, true).addCallback(queue_declared.bind(this));

        function queue_declared(queueName) {
            var msg = new BasicConsume(queueName, true,
                    true, this.handleReply.bind(this));
            this.amqp.rpc(msg).addCallback(consume_ok.bind(this));

            function consume_ok() {
                this.queueName = queueName;
                this.pendingRequests.each(this.sendPendingRequest.bind(this));
                this.pendingRequests = [];
            }
        }
    },

    sendRequest: function(request, txn) {
        var correlationId = this.nextCorrelationId++;
        this.transactionMap[correlationId] = txn;
        this.sendPendingRequest({request: request, correlationId: correlationId});
    },

    sendPendingRequest: function(p) {
        if (this.queueName == null) {
            this.pendingRequests.push(p);
        } else {
            this.amqp.basicPublish(
                    JSON.stringify(p.request),
                    this.exchange, this.routing_key, false, false,
            {reply_to: this.queueName, correlation_id: p.correlationId});
        }
    },

    handleReply: function(message) {
        var response = {responseText: message.message};
        var id = message.properties.correlation_id;
        this.transactionMap[id].receiveReply(response);
        delete this.transactionMap[id];
    }
});

AmqpJsonRpcTransaction = Class.create();
Object.extend(AmqpJsonRpcTransaction.prototype, JsonRpcTransaction.prototype);
Object.extend(AmqpJsonRpcTransaction.prototype, {
    sendRequest: function() {
        this.serviceUrl.sendRequest(this.buildRequest(), this);
    }
});

