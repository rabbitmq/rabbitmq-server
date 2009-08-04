-module(rabbitmq_http_server_app).

-behaviour(application).
-export([start/2,stop/1]).


%% @spec start(_Type, _StartArgs) -> ServerRet
%% @doc application start callback for rabbitmq_http_server.
start(_Type, _StartArgs) ->
    rabbitmq_http_server_sup:start_link().

%% @spec stop(_State) -> ServerRet
%% @doc application stop callback for rabbitmq_http_server.
stop(_State) ->
    ok.
