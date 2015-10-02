rabbitmq-web-dispatch
---------------------

rabbitmq-web-dispatch is a thin veneer around Cowboy that provides the
ability for multiple applications to co-exist on Cowboy
listeners. Applications can register static docroots or dynamic
handlers to be executed, dispatched by URL path prefix.

See http://www.rabbitmq.com/web-dispatch.html for information on
configuring web plugins.

The most general registration procedure is
`rabbit_web_dispatch:register_context_handler/5`.

This takes a dispatch list of the kind usually given to Cowboy, in compiled
form.
