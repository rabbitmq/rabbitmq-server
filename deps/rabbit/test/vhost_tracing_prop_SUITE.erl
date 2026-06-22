%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(vhost_tracing_prop_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [prop_no_lost_updates].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%% Property: when N vhosts are enabled concurrently while M distinct vhosts are
%% disabled concurrently, every enable takes effect and every disable takes
%% effect: no update is silently lost (see rabbitmq/rabbitmq-server#16755).
prop_no_lost_updates(_Config) ->
    ?assert(proper:quickcheck(no_lost_updates())).

no_lost_updates() ->
    ?FORALL({EnableVHosts, DisableVHosts},
            {list(non_empty(binary())), list(non_empty(binary()))},
            begin
                Enable  = lists:usort(EnableVHosts),
                %% Keep Disable disjoint from Enable so the expected final
                %% state is determinate regardless of execution order.
                Disable = lists:usort(DisableVHosts) -- Enable,
                application:set_env(rabbit, trace_vhosts, Disable),
                Parent = self(),
                EPids = [spawn(fun() ->
                    catch rabbit_trace:start(VH),
                    Parent ! {done, self()}
                end) || VH <- Enable],
                DPids = [spawn(fun() ->
                    catch rabbit_trace:stop(VH),
                    Parent ! {done, self()}
                end) || VH <- Disable],
                [receive {done, _} -> ok end || _ <- EPids],
                [receive {done, _} -> ok end || _ <- DPids],
                {ok, Final} = application:get_env(rabbit, trace_vhosts),
                lists:sort(Enable) =:= lists:sort(Final)
            end).
