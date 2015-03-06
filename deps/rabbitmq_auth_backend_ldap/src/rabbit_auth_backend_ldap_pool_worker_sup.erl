-module(rabbit_auth_backend_ldap_pool_worker_sup).

-export([start_link/2]).

-behaviour(supervisor).

-export([init/1]).

start_link(WorkerCount, MaxRestart) ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, [WorkerCount, MaxRestart]).

init([WorkerCount, {MaxR, MaxT}]) ->
  WorkerSpecs = [worker_spec(N) || N <- lists:seq(1, WorkerCount)],
  {ok, {{one_for_one, MaxR, MaxT}, WorkerSpecs}}.

worker_spec(N) ->
  Name = list_to_atom("rabbit_auth_backend_ldap_pool_worker_" ++ integer_to_list(N)),
  {Name,
   {rabbit_auth_backend_ldap_pool_worker, start_link, []},
   permanent,
   30,
   worker,
   [rabbit_auth_backend_ldap_pool_worker]}.
