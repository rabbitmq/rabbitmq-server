%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

%% Unit and property tests for the native STOMP subscribe-path authorization
%% helpers in rabbit_stomp_processor: the binding permission check and the
%% endpoint exchange existence probe. rabbit_access_control and rabbit_exchange
%% are mocked so the branches can be exercised without a running broker.
-module(unit_subscription_access_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-define(VHOST, <<"/">>).
-define(ITERATIONS, 500).

all() ->
    [%% check_subscription_binding_access/5
     direct_exchange_requires_write_and_read,
     write_on_destination_denied,
     read_on_source_exchange_denied,
     topic_exchange_requires_topic_read,
     topic_read_denied,
     missing_source_exchange_skips_topic_check,
     prop_permitted_iff_all_grants_present,
     %% ensure_exchange_exists/3
     probes_exchange_named_by_destination_in_connection_vhost,
     probe_disabled_by_default,
     missing_exchange_rejected].

init_per_testcase(_Testcase, Config) ->
    ok = meck:new(rabbit_access_control, [no_link]),
    ok = meck:new(rabbit_exchange, [no_link, passthrough]),
    Config.

end_per_testcase(_Testcase, _Config) ->
    ok = meck:unload(rabbit_exchange),
    ok = meck:unload(rabbit_access_control).

%% check_subscription_binding_access/5

direct_exchange_requires_write_and_read(_Config) ->
    Q = qname(<<"stomp.q">>),
    X = xname(<<"direct.x">>),
    ok = mock_exchange(X, direct),
    ok = mock_permitted([{Q, write}, {X, read}], _TopicOk = false),
    ?assertEqual(ok, check(Q, X, <<"key">>)),
    ?assertEqual(1, num_calls(Q, write)),
    ?assertEqual(1, num_calls(X, read)),
    ?assertEqual(0, topic_calls()).

write_on_destination_denied(_Config) ->
    Q = qname(<<"stomp.q">>),
    X = xname(<<"direct.x">>),
    ok = mock_exchange(X, direct),
    ok = mock_permitted([{X, read}], false),
    ?assertExit(#amqp_error{name = access_refused}, check(Q, X, <<"key">>)),
    ?assertEqual(1, num_calls(Q, write)),
    %% The source exchange read is only attempted after the destination write passes.
    ?assertEqual(0, num_calls(X, read)),
    ?assertEqual(0, topic_calls()).

read_on_source_exchange_denied(_Config) ->
    Q = qname(<<"stomp.q">>),
    X = xname(<<"direct.x">>),
    ok = mock_exchange(X, direct),
    ok = mock_permitted([{Q, write}], false),
    ?assertExit(#amqp_error{name = access_refused}, check(Q, X, <<"key">>)),
    ?assertEqual(1, num_calls(Q, write)),
    ?assertEqual(1, num_calls(X, read)),
    ?assertEqual(0, topic_calls()).

topic_exchange_requires_topic_read(_Config) ->
    Q = qname(<<"stomp.q">>),
    X = xname(<<"amq.topic">>),
    ok = mock_exchange(X, topic),
    ok = mock_permitted([{Q, write}, {X, read}], _TopicOk = true),
    ?assertEqual(ok, check(Q, X, <<"stock.eur">>)),
    ?assertEqual(1, num_calls(Q, write)),
    ?assertEqual(1, num_calls(X, read)),
    ?assertEqual(1, topic_calls()).

topic_read_denied(_Config) ->
    Q = qname(<<"stomp.q">>),
    X = xname(<<"amq.topic">>),
    ok = mock_exchange(X, topic),
    ok = mock_permitted([{Q, write}, {X, read}], _TopicOk = false),
    ?assertExit(#amqp_error{name = access_refused}, check(Q, X, <<"stock.eur">>)),
    ?assertEqual(1, topic_calls()).

%% An absent source exchange skips the topic check; rabbit_binding:add reports
%% the missing resource later.
missing_source_exchange_skips_topic_check(_Config) ->
    Q = qname(<<"stomp.q">>),
    X = xname(<<"gone.x">>),
    ok = mock_exchange_not_found(X),
    ok = mock_permitted([{Q, write}, {X, read}], false),
    ?assertEqual(ok, check(Q, X, <<"key">>)),
    ?assertEqual(0, topic_calls()).

%% A subscription binding is permitted exactly when the destination write, the
%% source read, and (for topic exchanges) the topic read are all granted. The
%% checks run in that order: a denial short-circuits the remaining ones.
prop_permitted_iff_all_grants_present(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun prop_permitted_iff_all_grants_present_/0, [], ?ITERATIONS).

prop_permitted_iff_all_grants_present_() ->
    ?FORALL(
       {WriteOk, ReadOk, TopicOk, IsTopic, QBin, XBin, RKey},
       {boolean(), boolean(), boolean(), boolean(),
        resource_name(), resource_name(), resource_name()},
       begin
           Q = qname(QBin),
           X = xname(XBin),
           Type = case IsTopic of true -> topic; false -> direct end,
           ok = mock_exchange(X, Type),
           Permitted = [{Q, write} || WriteOk] ++ [{X, read} || ReadOk],
           ok = mock_permitted(Permitted, TopicOk),
           ok = meck:reset(rabbit_access_control),
           Result = (catch check(Q, X, RKey)),
           Writes = num_calls(Q, write),
           Reads = num_calls(X, read),
           Topics = topic_calls(),
           case {WriteOk, ReadOk, IsTopic, TopicOk} of
               {false, _, _, _} ->
                   is_access_refused(Result)
                       andalso Writes =:= 1 andalso Reads =:= 0 andalso Topics =:= 0;
               {true, false, _, _} ->
                   is_access_refused(Result)
                       andalso Writes =:= 1 andalso Reads =:= 1 andalso Topics =:= 0;
               {true, true, false, _} ->
                   Result =:= ok
                       andalso Writes =:= 1 andalso Reads =:= 1 andalso Topics =:= 0;
               {true, true, true, true} ->
                   Result =:= ok
                       andalso Writes =:= 1 andalso Reads =:= 1 andalso Topics =:= 1;
               {true, true, true, false} ->
                   is_access_refused(Result)
                       andalso Writes =:= 1 andalso Reads =:= 1 andalso Topics =:= 1
           end
       end).

%% ensure_exchange_exists/3

%% Verifies the exchange resource argument order (vhost, exchange, name) is not
%% swapped.
probes_exchange_named_by_destination_in_connection_vhost(_Config) ->
    Name = <<"my.exchange">>,
    ok = meck:expect(rabbit_exchange, lookup_or_die,
                     fun(R) -> #exchange{name = R, type = direct} end),
    ok = ensure(?VHOST, Name, [{check_exchange, true}]),
    Expected = #resource{virtual_host = ?VHOST, kind = exchange, name = Name},
    Swapped = #resource{virtual_host = Name, kind = exchange, name = ?VHOST},
    ?assert(meck:called(rabbit_exchange, lookup_or_die, [Expected])),
    ?assertNot(meck:called(rabbit_exchange, lookup_or_die, [Swapped])).

probe_disabled_by_default(_Config) ->
    ok = meck:expect(rabbit_exchange, lookup_or_die, fun(R) -> #exchange{name = R} end),
    ok = ensure(?VHOST, <<"my.exchange">>, []),
    ?assertEqual(0, meck:num_calls(rabbit_exchange, lookup_or_die, '_')).

missing_exchange_rejected(_Config) ->
    ok = meck:expect(rabbit_exchange, lookup_or_die,
                     fun(R) ->
                             rabbit_misc:protocol_error(
                               not_found, "no ~ts", [rabbit_misc:rs(R)])
                     end),
    ?assertExit(#amqp_error{name = not_found},
                ensure(?VHOST, <<"absent">>, [{check_exchange, true}])).

resource_name() ->
    ?LET(N, non_empty(list(choose($a, $z))), list_to_binary(N)).

%% The check caches granted permissions in the process dictionary, so both
%% caches are cleared before every call to keep the mocked permission set
%% authoritative.
check(QName, ExchangeName, RoutingKey) ->
    erase(permission_cache),
    erase(topic_permission_cache),
    rabbit_stomp_processor:check_subscription_binding_access(
      QName, ExchangeName, RoutingKey, user(), authz_ctx()).

ensure(VHost, Name, Params) ->
    rabbit_stomp_processor:ensure_exchange_exists(VHost, Name, Params).

mock_exchange(#resource{} = ExchangeName, Type) ->
    meck:expect(rabbit_exchange, lookup,
                fun(Name) when Name =:= ExchangeName ->
                        {ok, #exchange{name = ExchangeName, type = Type}}
                end).

mock_exchange_not_found(#resource{} = ExchangeName) ->
    meck:expect(rabbit_exchange, lookup,
                fun(Name) when Name =:= ExchangeName ->
                        {error, not_found}
                end).

mock_permitted(Permitted, TopicOk) ->
    ok = meck:expect(
           rabbit_access_control, check_resource_access,
           fun(_User, Resource, Perm, _Context) ->
                   case lists:member({Resource, Perm}, Permitted) of
                       true ->
                           ok;
                       false ->
                           refuse(Resource)
                   end
           end),
    ok = meck:expect(
           rabbit_access_control, check_topic_access,
           fun(_User, Resource, _Perm, _Context) ->
                   case TopicOk of
                       true -> ok;
                       false -> refuse(Resource)
                   end
           end).

refuse(Resource) ->
    rabbit_misc:protocol_error(
      access_refused, "access to ~ts refused", [rabbit_misc:rs(Resource)]).

num_calls(Resource, Perm) ->
    meck:num_calls(rabbit_access_control, check_resource_access,
                   ['_', Resource, Perm, '_']).

topic_calls() ->
    meck:num_calls(rabbit_access_control, check_topic_access,
                   ['_', '_', '_', '_']).

is_access_refused({'EXIT', #amqp_error{name = access_refused}}) ->
    true;
is_access_refused(_) ->
    false.

qname(Name) ->
    #resource{virtual_host = ?VHOST, kind = queue, name = Name}.

xname(Name) ->
    #resource{virtual_host = ?VHOST, kind = exchange, name = Name}.

user() ->
    #user{username = <<"stompuser">>}.

authz_ctx() ->
    #{}.
