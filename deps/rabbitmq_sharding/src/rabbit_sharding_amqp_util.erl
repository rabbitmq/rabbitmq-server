-module(rabbit_sharding_amqp_util).

-include_lib("amqp_client/include/amqp_client.hrl").

-export([disposable_connection_calls/3]).

-define(MAX_CONNECTION_CLOSE_TIMEOUT, 10000).

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

ensure_connection_closed(Conn) ->
    catch amqp_connection:close(Conn, ?MAX_CONNECTION_CLOSE_TIMEOUT).

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

local_params(#exchange{name = #resource{virtual_host = VHost}} = X) ->
    Username = rabbit_sharding_util:username(X),
    case rabbit_access_control:check_user_login(Username, []) of
        {ok, _User}        -> #amqp_params_direct{username     = Username,
                                                  virtual_host = VHost};
        {refused, _M, _A}  -> exit({error, user_does_not_exist})
    end.