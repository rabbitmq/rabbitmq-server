rabbitmq-web-dispatch
---------------------

rabbitmq-web-dispatch is a thin veneer around mochiweb that provides the
ability for multiple applications to co-exist on mochiweb
listeners. Applications can register static docroots or dynamic
handlers to be executed, dispatched by URL path prefix.

See http://www.rabbitmq.com/mochiweb.html for information on
configuring web plugins.

The most general registration procedure is
`rabbit_web_dispatch:register_context_handler/5`. This takes a callback
procedure of the form

    loop(Request) ->
      ...

The module `rabbit_webmachine` provides a means of running more than
one webmachine in a VM, and understands rabbitmq-web-dispatch contexts. To
use it, supply a dispatch table term of the kind usually given to
webmachine in the file `priv/dispatch.conf`.

`setup/{1,2}` in the same module allows some global configuration of
webmachine logging and error handling.
