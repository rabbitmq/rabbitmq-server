%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

all() ->
    [filter_user_tag_representation].

%% `#user.tags` may hold binaries (the internal backend, which avoids
%% interning admin-supplied tag strings) or atoms (LDAP, HTTP, OAuth2
%% backends); both must grant a monitor visibility of other users'
%% connection and channel stats.
filter_user_tag_representation(_Config) ->
    List = [[{user, <<"other">>}], [{user, <<"me">>}]],

    MonitorBin = #user{username = <<"me">>, tags = [<<"monitoring">>]},
    ?assertEqual(List, rabbit_mgmt_data:filter_user(List, MonitorBin)),

    MonitorAtom = #user{username = <<"me">>, tags = [monitoring]},
    ?assertEqual(List, rabbit_mgmt_data:filter_user(List, MonitorAtom)),

    NonMonitor = #user{username = <<"me">>, tags = [<<"management">>]},
    ?assertEqual([[{user, <<"me">>}]],
                 rabbit_mgmt_data:filter_user(List, NonMonitor)),
    ok.
