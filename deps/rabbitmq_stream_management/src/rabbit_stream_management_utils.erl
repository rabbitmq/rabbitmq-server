%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_management_utils).

-export([keep_stream_connections/1,
         keep_tracked_stream_connections/1,
         is_stream_connection/1,
         is_feature_flag_enabled/0]).

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

is_feature_flag_enabled() ->
    rabbit_feature_flags:is_enabled(stream_queue).
