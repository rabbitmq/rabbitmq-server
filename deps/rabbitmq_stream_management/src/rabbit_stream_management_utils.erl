%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_stream_management_utils).

-export([keep_stream_connections/1,
         keep_tracked_stream_connections/1,
         is_stream_connection/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

keep_stream_connections(Connections) ->
    lists:filter(fun is_stream_connection/1, Connections).

is_stream_connection(Connection) ->
    case lists:keyfind(protocol, 1, Connection) of
        {protocol, <<"stream">>} ->
            true;
        _ ->
            false
    end.

keep_tracked_stream_connections(Connections) ->
    lists:filter(fun (#tracked_connection{protocol = <<"stream">>}) ->
                         true;
                     (_) ->
                         false
                 end,
                 Connections).
