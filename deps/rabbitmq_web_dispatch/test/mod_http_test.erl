-module(mod_http_test).

-include("rfc4627_jsonrpc.hrl").

-behaviour(application).
-export([start/2,stop/1]).

start(_Type, _StartArgs) ->
    mod_http_web:register_docroot("mod_http_test","priv/www"),
    {ok, Pid} = gen_server:start_link(mod_http_test_server, [], []),
    rfc4627_jsonrpc:register_service
      (Pid,
       rfc4627_jsonrpc:service(<<"test">>,
                   <<"urn:uuid:afe1b4b5-23b0-4964-a74a-9168535c96b2">>,
                   <<"1.0">>,
                   [#service_proc{name = <<"test_proc">>,
                          idempotent = true,
                          params = [#service_proc_param{name = <<"value">>,
                                        type = <<"str">>}]}])),
    {ok, Pid}.

stop(_State) ->
    ok.
