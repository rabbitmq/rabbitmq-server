%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_web_mqtt_app).

-behaviour(application).
-export([start/2, prep_stop/1, stop/1]).

%% Dummy supervisor - see Ulf Wiger's comment at
%% http://erlang.2086793.n4.nabble.com/initializing-library-applications-without-processes-td2094473.html
-behaviour(supervisor).
-export([init/1]).

%%----------------------------------------------------------------------------

-spec start(_, _) -> {ok, pid()}.
start(_Type, _StartArgs) ->
    mqtt_init(),
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec prep_stop(term()) -> term().
prep_stop(State) ->
    ranch:stop_listener(web_mqtt),
    State.

-spec stop(_) -> ok.
stop(_State) ->
    ok.

init([]) -> {ok, {{one_for_one, 1, 5}, []}}.

%%----------------------------------------------------------------------------

mqtt_init() ->
    CowboyOpts0 = get_env(cowboy_opts, []),

    Routes = cowboy_router:compile([{'_', [
        {"/ws", rabbit_web_mqtt_handler, []}
    ]}]),
    CowboyOpts = [
        {env, [{dispatch, Routes}]},
        {middlewares, [cowboy_router, rabbit_web_mqtt_middleware, cowboy_handler]}
    |CowboyOpts0],

    TCPConf0 = [{connection_type, supervisor}|get_env(tcp_config, [])],
    TCPConf = case proplists:get_value(port, TCPConf0) of
        undefined -> [{port, 15675}|TCPConf0];
        _ -> TCPConf0
    end,
    TCPPort = proplists:get_value(port, TCPConf),

    {ok, _} = ranch:start_listener(web_mqtt, get_env(num_tcp_acceptors, 10),
        ranch_tcp, TCPConf,
        rabbit_web_mqtt_connection_sup, CowboyOpts),
    listener_started('http/web-mqtt', TCPConf),
    rabbit_log:info("rabbit_web_mqtt: listening for HTTP connections on ~s:~w~n",
                    ["0.0.0.0", TCPPort]),

    case get_env(ssl_config, []) of
        [] ->
            ok;
        SSLConf0 ->
            rabbit_networking:ensure_ssl(),
            SSLPort = proplists:get_value(port, SSLConf0),
            SSLConf = [{connection_type, supervisor}|SSLConf0],

            {ok, _} = ranch:start_listener(web_mqtt_secure, get_env(num_ssl_acceptors, 1),
                ranch_ssl, SSLConf,
                rabbit_web_mqtt_connection_sup, CowboyOpts),
            listener_started('https/web-mqtt', TCPConf),
            rabbit_log:info("rabbit_web_mqtt: listening for HTTPS connections on ~s:~w~n",
                            ["0.0.0.0", SSLPort])
    end,
    ok.

listener_started(Protocol, Listener) ->
    Port = rabbit_misc:pget(port, Listener),
    [rabbit_networking:tcp_listener_started(Protocol, Listener,
                                            IPAddress, Port)
     || {IPAddress, _Port, _Family}
        <- rabbit_networking:tcp_listener_addresses(Port)],
    ok.

get_env(Key, Default) ->
    case application:get_env(rabbitmq_web_mqtt, Key) of
        undefined -> Default;
        {ok, V}   -> V
    end.
