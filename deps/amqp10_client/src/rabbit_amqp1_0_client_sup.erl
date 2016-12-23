-module(rabbit_amqp1_0_client_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([start_connection_sup/2]).
-export([init/1]).

-define(CHILD(Id, Mod, Type, Args), {Id, {Mod, start_link, Args},
                                     temporary, infinity, Type, [Mod]}).

-spec start_connection_sup(inet:socket_address() | inet:hostname(), inet:port_number()) ->
    supervisor:startchild_ret().
start_connection_sup(Addr, Port) ->
    {ok, ConnSup} = supervisor:start_child(?MODULE, [Addr, Port]),
    Children = supervisor:which_children(ConnSup),
    {_Id, ConnPid, _, _} = lists:keyfind(connection, 1, Children),
    {ok, ConnPid}.

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Template = ?CHILD(unused_id, rabbit_amqp1_0_client_connection_sup,
                      supervisor, []),
    {ok, {{simple_one_for_one, 1, 5}, [Template]}}.
