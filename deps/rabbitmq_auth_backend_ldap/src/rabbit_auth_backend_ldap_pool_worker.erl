-module(rabbit_auth_backend_ldap_pool_worker).

-export([start_link/0,
         run/0]).

%% how long we're willing to wait for the coordinator before giving up
-define(TIMEOUT, 5000).

start_link() ->
  Pid = spawn_link(?MODULE, run, []),
  {ok, Pid}.

run() ->
  rabbit_auth_backend_ldap_pool_coord:hello_from_worker(?TIMEOUT),
  run1(undefined).

run1(State) ->
  {From, Work} = rabbit_auth_backend_ldap_pool_coord:get_work(?TIMEOUT),
  {Answer, State1} = apply(Work, [State]),
  rabbit_auth_backend_ldap_pool_coord:done_work(From, Answer, ?TIMEOUT),
  run1(State1).
