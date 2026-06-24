%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

%% Unit and property tests for the dead-letter-exchange permission check used by
%% the Native STOMP queue.declare path. rabbit_access_control is mocked so the
%% authorization branches can be exercised without a running broker.
-module(unit_dead_letter_access_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-define(VHOST, <<"/">>).
-define(DLX_HEADER, <<"x-dead-letter-exchange">>).
-define(ITERATIONS, 500).

all() ->
    [absent_dlx_arg_skips_checks,
     invalid_dlx_arg_type_rejected,
     both_permissions_granted,
     queue_read_denied_skips_dlx_write,
     dlx_write_denied,
     prop_read_then_write].

init_per_testcase(_Testcase, Config) ->
    ok = meck:new(rabbit_access_control, [no_link]),
    Config.

end_per_testcase(_Testcase, _Config) ->
    ok = meck:unload(rabbit_access_control).

absent_dlx_arg_skips_checks(_Config) ->
    ok = mock_permitted([]),
    ?assertEqual(ok, check(qname(<<"stomp.q">>), [])),
    ?assertEqual(0, total_calls()).

invalid_dlx_arg_type_rejected(_Config) ->
    ok = mock_permitted([]),
    Args = [{?DLX_HEADER, long, 42}],
    ?assertExit(#amqp_error{name = precondition_failed},
                check(qname(<<"stomp.q">>), Args)),
    ?assertEqual(0, total_calls()).

both_permissions_granted(_Config) ->
    Q = qname(<<"stomp.q">>),
    X = xname(<<"stomp.dlx">>),
    ok = mock_permitted([{Q, read}, {X, write}]),
    ?assertEqual(ok, check(Q, dlx_args(<<"stomp.dlx">>))),
    ?assertEqual(1, num_calls(Q, read)),
    ?assertEqual(1, num_calls(X, write)).

queue_read_denied_skips_dlx_write(_Config) ->
    Q = qname(<<"stomp.q">>),
    X = xname(<<"stomp.dlx">>),
    ok = mock_permitted([{X, write}]),
    ?assertExit(#amqp_error{name = access_refused},
                check(Q, dlx_args(<<"stomp.dlx">>))),
    ?assertEqual(1, num_calls(Q, read)),
    ?assertEqual(0, num_calls(X, write)).

dlx_write_denied(_Config) ->
    Q = qname(<<"stomp.q">>),
    X = xname(<<"stomp.dlx">>),
    ok = mock_permitted([{Q, read}]),
    ?assertExit(#amqp_error{name = access_refused},
                check(Q, dlx_args(<<"stomp.dlx">>))),
    ?assertEqual(1, num_calls(Q, read)),
    ?assertEqual(1, num_calls(X, write)).

%% Success requires both grants, and the dead-letter exchange write is only
%% attempted after the queue read passes.
prop_read_then_write(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun prop_read_then_write_/0, [], ?ITERATIONS).

prop_read_then_write_() ->
    ?FORALL(
       {ReadOk, WriteOk, QBin, XBin},
       {boolean(), boolean(), resource_name(), resource_name()},
       begin
           Q = qname(QBin),
           X = xname(XBin),
           Permitted = [{Q, read} || ReadOk] ++ [{X, write} || WriteOk],
           ok = mock_permitted(Permitted),
           ok = meck:reset(rabbit_access_control),
           Result = (catch check(Q, dlx_args(XBin))),
           Reads = num_calls(Q, read),
           Writes = num_calls(X, write),
           case {ReadOk, WriteOk} of
               {true, true} ->
                   Result =:= ok andalso Reads =:= 1 andalso Writes =:= 1;
               {false, _} ->
                   is_access_refused(Result) andalso Reads =:= 1 andalso Writes =:= 0;
               {true, false} ->
                   is_access_refused(Result) andalso Reads =:= 1 andalso Writes =:= 1
           end
       end).

resource_name() ->
    ?LET(N, non_empty(list(choose($a, $z))), list_to_binary(N)).

%% The wrapper caches granted permissions in the process dictionary, so it is
%% cleared before every call to keep mocked permission sets authoritative.
check(QName, Args) ->
    erase(permission_cache),
    rabbit_stomp_processor:check_dead_letter_exchange_access(
      QName, Args, user(), authz_ctx()).

mock_permitted(Permitted) ->
    meck:expect(
      rabbit_access_control, check_resource_access,
      fun(_User, Resource, Perm, _Context) ->
              case lists:member({Resource, Perm}, Permitted) of
                  true ->
                      ok;
                  false ->
                      rabbit_misc:protocol_error(
                        access_refused, "access to ~ts refused",
                        [rabbit_misc:rs(Resource)])
              end
      end).

total_calls() ->
    meck:num_calls(rabbit_access_control, check_resource_access,
                   ['_', '_', '_', '_']).

num_calls(Resource, Perm) ->
    meck:num_calls(rabbit_access_control, check_resource_access,
                   ['_', Resource, Perm, '_']).

is_access_refused({'EXIT', #amqp_error{name = access_refused}}) ->
    true;
is_access_refused(_) ->
    false.

qname(Name) ->
    #resource{virtual_host = ?VHOST, kind = queue, name = Name}.

xname(Name) ->
    #resource{virtual_host = ?VHOST, kind = exchange, name = Name}.

dlx_args(XBin) ->
    [{?DLX_HEADER, longstr, XBin}].

user() ->
    #user{username = <<"stompuser">>}.

authz_ctx() ->
    #{}.
