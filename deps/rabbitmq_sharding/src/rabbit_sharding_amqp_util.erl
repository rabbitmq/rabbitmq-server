-module(rabbit_sharding_amqp_util).

-include_lib("amqp_client/include/amqp_client.hrl").

-export([disposable_channel_call/2, disposable_channel_call/3,
         disposable_connection_call/3, disposable_connection_calls/3,
         ensure_connection_closed/1]).

-define(MAX_CONNECTION_CLOSE_TIMEOUT, 10000).

%%----------------------------------------------------------------------------

open(X) ->
    case amqp_connection:start(local_params(X)) of
        {ok, Conn} -> case amqp_connection:open_channel(Conn) of
                          {ok, Ch} -> {ok, Conn, Ch};
                          E        -> catch amqp_connection:close(Conn),
                                      E
                      end;
        E -> E
    end.

ensure_channel_closed(Ch) -> catch amqp_channel:close(Ch).

ensure_connection_closed(Conn) ->
    catch amqp_connection:close(Conn, ?MAX_CONNECTION_CLOSE_TIMEOUT).

%%----------------------------------------------------------------------------

disposable_channel_call(Conn, Method) ->
    disposable_channel_call(Conn, Method, fun(_, _) -> ok end).

disposable_channel_call(Conn, Method, ErrFun) ->
    {ok, Ch} = amqp_connection:open_channel(Conn),
    try
        amqp_channel:call(Ch, Method)
    catch exit:{{shutdown, {server_initiated_close, Code, Text}}, _} ->
            ErrFun(Code, Text)
    after
        ensure_channel_closed(Ch)
    end.

disposable_connection_call(X, Method, ErrFun) ->
    case open(X) of
        {ok, Conn, Ch} ->
            try
                amqp_channel:call(Ch, Method)
            catch exit:{{shutdown, {connection_closing,
                                    {server_initiated_close, Code, Txt}}}, _} ->
                    ErrFun(Code, Txt)
            after
                ensure_connection_closed(Conn)
            end;
        E ->
            E
    end.

disposable_connection_calls(X, Methods, ErrFun) ->
    case open(X) of
        {ok, Conn, Ch} ->
            try
                [amqp_channel:call(Ch, Method) || Method <- Methods]
            catch exit:{{shutdown, {connection_closing,
                                    {server_initiated_close, Code, Txt}}}, _} ->
                    ErrFun(Code, Txt)
            after
                ensure_connection_closed(Conn)
            end;
        E ->
            E
    end.

%%----------------------------------------------------------------------------

local_params(#exchange{name = #resource{virtual_host = VHost}} = X) ->
    Username = rabbit_sharding_util:username(X),
    case rabbit_access_control:check_user_login(Username, []) of
        {ok, _User}        -> #amqp_params_direct{username     = Username,
                                                  virtual_host = VHost};
        {refused, _M, _A}  -> exit({error, user_does_not_exist})
    end.