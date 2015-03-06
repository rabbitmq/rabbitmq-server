-module(rabbit_auth_backend_ldap_pool_test).

-include_lib("eunit/include/eunit.hrl").

-define(CLIENT_COUNT, 10).
-define(WORKER_COUNT, 10).
-define(CLIENT_OPS, 100).
-define(MAX_SLEEP, 3).
%% expect a lot of restarts
-define(MAX_RESTART, {100000, 1}).

%% a kind of stress test where we have some clients and workers, and the clients
%% submit a bunch of random work, some of which causes the workers to fail.
marches_inexorably_forward_in_a_chaotic_universe_test_() ->
  {setup,
   fun () ->
      erlang:process_flag(trap_exit, true),
      rabbit_auth_backend_ldap_pool_coord:start_link(),
      rabbit_auth_backend_ldap_pool_worker_sup:start_link(?WORKER_COUNT, ?MAX_RESTART),
      ok
   end,
   fun (_) ->
       rabbit_auth_backend_ldap_pool_coord:stop(5000)
   end,
   fun () ->
      Clients = make_clients(),
      lists:foreach(fun await_client/1, Clients)
   end}.

make_clients() ->
  [spawn_link(fun () ->
                  random:seed(N, N, N),
                  march(?CLIENT_OPS),
                  receive
                    {ping, Pid} -> Pid ! {pong, self()}
                  end
              end)
   || N <- lists:seq(1, ?CLIENT_COUNT)].

await_client(Client) ->
  Client ! {ping, self()},
  receive
    {pong, Client} -> ok;
    {'EXIT', E} -> ?debugVal(E);
    Other -> ?debugVal(Other)
  end.

%% generate a {Work, Check} where Work is to be sent to the worker and
%% Check is how to verify the response
make_work() ->
  N = random:uniform(?MAX_SLEEP) - 1,
  if
    N > 0 ->
      {fun (State) -> timer:sleep(N), {N, State} end,
       fun (A) -> ?assertEqual(N, A) end};
    true ->
      {fun (_) -> exit(cest_la_vie) end,
       fun (A) -> ?assertMatch({'EXIT', _}, A) end}
  end.

%% send N random pieces of work to the workers
march(N) when N > 0 ->
  {Work, Check} = make_work(),
  Answer = (catch rabbit_auth_backend_ldap_pool_coord:do_work(Work, infinity)),
  apply(Check, [Answer]),
  march(N - 1);

march(0) -> ok.
