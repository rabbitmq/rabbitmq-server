-module(rabbit_amqp_sole_conn).

-rabbit_boot_step({?MODULE,
    [{description, "AMQP 1.0 JMS unique connection enforcement tracker"},
     {mfa,         {rabbit_sup, start_restartable_child, [?MODULE]}},
     {requires,    pre_boot},
     {enables,     external_infrastructure}]}).

%% API
-export([start_link/0, acquire/3, release/2]).

-type vhost() :: binary().
-type container_id() :: binary().

-spec acquire(vhost(), container_id(), pid()) -> 
    ok | {error, refuse_connection}.
acquire(VHost, ContainerId, ConnectionPid) ->
    gen_server:call(?MODULE, {acquire, VHost, ContainerId, ConnectionPid}).

-spec release(vhost(), container_id()) -> ok.
release(VHost, ContainerId) ->
    gen_server:call(?MODULE, {release, VHost, ContainerId}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, rabbit_amqp_sole_conn_local, [], []).
