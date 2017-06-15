-module(amqp_util).

-include("amqp_client_internal.hrl").

-export([call_timeout/0]).

call_timeout() ->
    case get(gen_server_call_timeout) of
        undefined ->
            Timeout = rabbit_misc:get_env(amqp_client,
                                          gen_server_call_timeout,
                                          ?CALL_TIMEOUT),
            put(gen_server_call_timeout, Timeout),
            Timeout;
        Timeout ->
            Timeout
    end.
