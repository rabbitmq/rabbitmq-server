JsonRpcRequestId = 1;

JsonRpcTransaction = Class.create();
Object.extend(JsonRpcTransaction.prototype,
{
    initialize: function(serviceUrl, methodName, params, options) {
	this.options = {
	    debug: false,
	    timeout: 0 /* milliseconds; zero means "do not specify" */
	};
	Object.extend(this.options, options || {});
	this.serviceUrl = serviceUrl;
	this.methodName = methodName;
	this.params = params;
	this.error = null;
	this.reply = null;
	this.replyReady = 0;
	this.callbacks = [];
	this.errorCallbacks = [];
	this.sendRequest();
    },

    buildRequest: function() {
	return { version: "1.1",
		 id: JsonRpcRequestId++,
		 method: this.methodName,
		 params: this.params };
    },

    sendRequest: function() {
	var headers = ['Content-type', 'application/json',
		       'Accept', 'application/json'];
	if (this.options.timeout) {
	    headers.push('X-JSON-RPC-Timeout', this.options.timeout);
	}
	this.request =
	    new Ajax.Request(this.serviceUrl,
			     { method: 'post',
			       requestHeaders: headers,
			       postBody: JSON.stringify(this.buildRequest()),
			       onComplete: this.receiveReply.bind(this) });
    },

    receiveReply: function(ajaxRequest) {
	var response = JSON.parse(ajaxRequest.responseText);
	if (response.error) {
	    if (this.options.debug) {
		alert("JsonRPC error:\n" +
		      "Service: " + JSON.stringify(this.serviceUrl) + "\n" +
		      "Method: " + JSON.stringify(this.methodName) + "\n" +
		      "Params: " + JSON.stringify(this.params) + "\n" +
		      "Response: " + JSON.stringify(response) + "\n");
	    }
	    this.error = response.error;
	    this.errorCallbacks.each(function (cb) {
					 try { cb(response.error, true); }
					 catch (err) {}
				     });
	} else {
	    var reply = response.result;
	    this.reply = reply;
	    this.replyReady = 1;
	    this.callbacks.each(function (cb) {
				    try { cb(reply, false); }
				    catch (err) {}
				});
	}
    },

    addCallback: function(cb) {
	this.callbacks.push(cb);
	if (this.replyReady) {
	    try { cb(this.reply, false); }
	    catch (err) {}
	}
	return this;
    },

    addErrorCallback: function(cb) {
	this.errorCallbacks.push(cb);
	if (this.error) {
	    try { cb(this.error, true); }
	    catch (err) {}
	}
	return this;
    }
});

JsonRpcService = Class.create();
Object.extend(JsonRpcService.prototype,
{
    initialize: function(serviceUrl, onReady, options) {
	this.options = {
	    transactionClass: JsonRpcTransaction,
	    timeout: 0, /* milliseconds; zero means "do not specify" */
	    debug: false
	};
	Object.extend(this.options, options || {});
	this.serviceUrl = serviceUrl;
	var svc = this;
	var txn = new (this.options.transactionClass)(serviceUrl,
						      "system.describe",
						      [],
						      {debug: this.options.debug});
	txn.addCallback(receiveServiceDescription);
	function receiveServiceDescription(sd) {
	    svc.serviceDescription = sd;
	    svc.serviceDescription.procs.each(svc.installGenericProxy.bind(svc));
	    onReady();
	}
    },

    installGenericProxy: function(desc) {
	this[desc.name] = function () {
	    var actuals = $A(arguments);
	    return new (this.options.transactionClass)(this.serviceUrl,
						       desc.name,
						       actuals,
						       {
							   debug: this.options.debug,
							   timeout: this.options.timeout
						       });
	};
    }
});
