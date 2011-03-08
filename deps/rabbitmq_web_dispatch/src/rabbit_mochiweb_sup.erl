-module(rabbit_mochiweb_sup).

-behaviour(supervisor).

-define(SUP, ?MODULE).

%% External exports
-export([start_link/1, upgrade/1, ensure_listener/1]).

%% supervisor callbacks
-export([init/1]).

%% @spec start_link() -> ServerRet
%% @doc API for starting the supervisor.
start_link(Instances) ->
    supervisor:start_link({local, ?SUP}, ?MODULE, [Instances]).

%% @spec upgrade([instance()]) -> ok
%% @doc Add processes if necessary.
upgrade(Instances) ->
    {ok, {_, Specs}} = init([Instances]),

    Old = sets:from_list(
            [Name || {Name, _, _, _} <- supervisor:which_children(?MODULE)]),
    New = sets:from_list([Name || {Name, _, _, _, _, _} <- Specs]),
    Kill = sets:subtract(Old, New),

    sets:fold(fun (Id, ok) ->
                      supervisor:terminate_child(?SUP, Id),
                      supervisor:delete_child(?SUP, Id),
                      ok
              end, ok, Kill),

    [supervisor:start_child(?SUP, Spec) || Spec <- Specs],
    ok.

ensure_listener({Instance, Spec}) ->
    Child = {{rabbit_mochiweb_web, Instance},
             {rabbit_mochiweb_web, start, [{Instance, Spec}]},
             permanent, 5000, worker, dynamic},
    case supervisor:start_child(?SUP, Child) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            {ok, Pid}
    end.

%% @spec init([[instance()]]) -> SupervisorTree
%% @doc supervisor callback.
init([Instances]) ->
    Registry = {rabbit_mochiweb_registry,
                {rabbit_mochiweb_registry, start_link, [Instances]},
                permanent, 5000, worker, dynamic},
    {ok, {{one_for_one, 10, 10}, [Registry]}}.
