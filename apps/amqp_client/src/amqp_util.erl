-module(amqp_util).

-include("amqp_client_internal.hrl").

-export([call_timeout/0, update_call_timeout/1, safe_call_timeout/1]).

call_timeout() ->
    case get(gen_server_call_timeout) of
        undefined ->
            Timeout = rabbit_misc:get_env(amqp_client,
                                          gen_server_call_timeout,
                                          safe_call_timeout(60000)),
            put(gen_server_call_timeout, Timeout),
            Timeout;
        Timeout ->
            Timeout
    end.

update_call_timeout(Timeout) ->
    application:set_env(amqp_client, gen_server_call_timeout, Timeout),
    put(gen_server_call_timeout, Timeout),
    ok.

safe_call_timeout(Threshold) ->
    Threshold + ?CALL_TIMEOUT_DEVIATION.
