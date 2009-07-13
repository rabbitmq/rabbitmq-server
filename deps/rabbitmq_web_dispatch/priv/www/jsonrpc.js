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

JsonRpcRequestId = 1;

JsonRpcTransaction = Class.create();
Object.extend(JsonRpcTransaction.prototype,
{
    initialize: function(serviceUrl, methodName, params, options) {
	this.options = {
	    debug: false,
	    debugLogger: alert,
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
	var req = this.buildRequest();
	//this.debugLog({requestX: req});
	this.request =
	    new Ajax.Request(this.serviceUrl,
			     { method: 'post',
			       requestHeaders: headers,
			       postBody: JSON.stringify(req),
			       onComplete: this.receiveReply.bind(this) });
    },

    debugLog: function(x) {
	if (this.options.debug) {
	    this.options.debugLogger(x);
	}
    },

    receiveReply: function(ajaxRequest) {
	var response = JSON.parse(ajaxRequest.responseText);
	//this.debugLog({responseX: response});
	if (response.error) {
	    if (this.options.debug) {
		this.debugLog("JsonRPC error:" +
			      "\nService: " + JSON.stringify(this.serviceUrl) +
			      "\nMethod: " + JSON.stringify(this.methodName) +
			      "\nParams: " + JSON.stringify(this.params) +
			      "\nResponse: " + JSON.stringify(response).replace(/\\n/g, "\n"));
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

    addReplyTransformer: function(xformer) {
	var oldAddCallback = this.addCallback.bind(this);
	this.addCallback = function(cb) {
	    return oldAddCallback(function(reply, is_error) {
				      cb(is_error ? reply : xformer(reply), is_error);
				  });
	}
	return this;
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
	    debug: false,
	    debugLogger: alert
	};
	Object.extend(this.options, options || {});
	this.serviceUrl = serviceUrl;
	var svc = this;
	var txn = new (this.options.transactionClass)(serviceUrl,
						      "system.describe",
						      [],
						      {debug: this.options.debug,
						       debugLogger: this.options.debugLogger});
	txn.addCallback(receiveServiceDescription);
	function receiveServiceDescription(sd) {
	    svc.serviceDescription = sd;
	    svc.serviceDescription.procs.each(svc.installGenericProxy.bind(svc));
	    onReady();
	}
    },

    installGenericProxy: function(desc) {
	if (this.options.debug) {
	    this.options.debugLogger({installGenericProxy: desc});
	}
	this[desc.name] = function () {
	    var actuals = $A(arguments);
	    while (actuals.length < desc.params.length) {
		actuals.push(null);
	    }
	    return new (this.options.transactionClass)(this.serviceUrl,
						       desc.name,
						       actuals,
						       {
							   debug: this.options.debug,
							   debugLogger: this.options.debugLogger,
							   timeout: this.options.timeout
						       });
	};
    }
});
