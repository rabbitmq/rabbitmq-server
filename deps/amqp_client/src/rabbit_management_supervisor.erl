-module(rabbit_management_supervisor).

-include("amqp_client.hrl").

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init(_) ->
    X = <<"x">>,
    BindKey = <<"a.b.c.*">>,
    RoutingKey = <<"a.b.c.d">>,
    Realm = <<"/data">>,
    Q = <<"a.b.c">>,
    ServerName = rabbit_management,
    EventHandlerName = rabbit_management_event_handler,
    TypeMapping = integration_test_util:encoding_state(),
    Username = "guest",
    Password = "guest",
    BrokerConfig = #broker_config{exchange = X,
                                  routing_key = RoutingKey,
                                  queue = Q,
                                  realm = Realm,
                                  bind_key = BindKey},
    Args = [ServerName,
            TypeMapping,
            Username,
            Password,
            BrokerConfig],
    RabbitManagement
        = {rabbit_management,
            {gen_server, start_link, [amqp_rpc_handler, Args, []]},
             permanent, brutal_kill, worker, [amqp_rpc_handler]},

    {ok, { {one_for_one, 10, 1}, [RabbitManagement]}}.
