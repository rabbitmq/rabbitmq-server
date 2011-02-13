-module(rabbit_mochiweb_sup).

-behaviour(supervisor).

%% External exports
-export([start_link/1, upgrade/1]).

%% supervisor callbacks
-export([init/1]).

%% @spec start_link() -> ServerRet
%% @doc API for starting the supervisor.
start_link(Instances) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Instances]).

%% @spec upgrade([instance()]) -> ok
%% @doc Add processes if necessary.
upgrade(Instances) ->
    {ok, {_, Specs}} = init([Instances]),

    Old = sets:from_list(
            [Name || {Name, _, _, _} <- supervisor:which_children(?MODULE)]),
    New = sets:from_list([Name || {Name, _, _, _, _, _} <- Specs]),
    Kill = sets:subtract(Old, New),

    sets:fold(fun (Id, ok) ->
                      supervisor:terminate_child(?MODULE, Id),
                      supervisor:delete_child(?MODULE, Id),
                      ok
              end, ok, Kill),

    [supervisor:start_child(?MODULE, Spec) || Spec <- Specs],
    ok.

%% @spec init([[instance()]]) -> SupervisorTree
%% @doc supervisor callback.
init([Specs]) ->
    Registry = {rabbit_mochiweb_registry,
                {rabbit_mochiweb_registry, start_link, [Specs]},
                permanent, 5000, worker, dynamic},
    Webs = [{{rabbit_mochiweb_web, Instance},
             {rabbit_mochiweb_web, start, [{Instance, Spec}]},
             permanent, 5000, worker, dynamic} ||
               {Instance, Spec} <- Specs],

    Processes = [Registry | Webs],
    {ok, {{one_for_one, 10, 10}, Processes}}.
