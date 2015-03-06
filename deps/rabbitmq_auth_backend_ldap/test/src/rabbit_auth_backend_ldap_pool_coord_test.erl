%% unit tests for rabbit_auth_backend_ldap_pool_coord

-module(rabbit_auth_backend_ldap_pool_coord_test).

-include_lib("eunit/include/eunit.hrl").

%% support gubbins -------------------------------------------------------------

-define(IT, rabbit_auth_backend_ldap_pool_coord).

-define(assertEventuallyEqual(E, A), assertEventuallyEqual(E, fun() -> A end)).

assertEventuallyEqual(E, A) ->
  case apply(A, []) of
    E -> ok;
    Other ->
      receive
      after
        50 -> assertEventuallyEqual(E, A)
      end
  end.

get_info_for_testing(Key) ->
  Info = ?IT:get_info_for_testing(),
  proplists:get_value(Key, Info).

recv() ->
  receive
    X -> X
  after
    1000 -> timeout
  end.

%% the actual tests! -----------------------------------------------------------

worker_arrives_first() ->
  Worker = spawn(fun () ->
                     ?IT:hello_from_worker(infinity),
                     {From, Work} = ?IT:get_work(infinity),
                     ?IT:done_work(From, {i_have_done, Work}, infinity)
                 end),
  ?assertEventuallyEqual(1, get_info_for_testing(idle_worker_count)),
  R = ?IT:do_work(some_work, infinity),
  ?assertEqual(R, {i_have_done, some_work}).

work_arrives_first() ->
  Self = self(),
  Client = spawn(fun() ->
                     Response = ?IT:do_work(some_work, infinity),
                     Self ! {client_finished, Response}
                 end),
  ?assertEventuallyEqual(1, get_info_for_testing(work_queue_count)),
  {From, Work} = ?IT:get_work(infinity),
  ?assertMatch({Client, _}, From),
  ?assertEqual(some_work, Work),
  ?IT:done_work(From, {i_have_done, Work}, infinity),
  ?assertEqual(0, get_info_for_testing(work_queue_count)),
  {client_finished, {i_have_done, Work}} = recv().

busy_worker_fails() ->
  Worker = spawn(fun () ->
                     ?debugHere,
                     ?IT:hello_from_worker(infinity),
                     ?debugHere,
                     {From, Work} = ?IT:get_work(infinity),
                     ?debugHere
                 end),
  R = (catch ?IT:do_work(some_work, infinity)),
  ?assertMatch({'EXIT', _}, R).

idle_worker_fails() ->
  Worker = spawn(fun () ->
                     ?IT:hello_from_worker(infinity),
                     {From, Work} = ?IT:get_work(infinity)
                 end),
  ?assertEventuallyEqual(1, get_info_for_testing(idle_worker_count)),
  erlang:exit(Worker, cest_la_vie),
  ?assertEventuallyEqual(0, get_info_for_testing(idle_worker_count)).

all_test_() ->
  {inorder,
    {foreach,
     fun () -> ?IT:start_link() end,
     fun (_) -> ?IT:stop(5000) end,
     [fun work_arrives_first/0,
      fun worker_arrives_first/0,
      fun busy_worker_fails/0,
      fun idle_worker_fails/0]}}.
