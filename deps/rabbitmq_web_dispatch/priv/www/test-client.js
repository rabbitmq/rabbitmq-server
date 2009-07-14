function log() {
    $A(arguments).each(function (arg) {
			   $("testOutput").appendChild
			     (document.createTextNode(JSON.stringify(arg) + "\n"));
		       });
}

function testMain() {
    log("Starting.");
    var testService = new JsonRpcService(document.location + "rpc/test", onReady);
    function onReady() {
	testService.test_proc("Hello, world!").addCallback(log);
    }
}
