%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mqtt_vhost).
-behaviour(rabbit_vhost_deletion).

-include_lib("kernel/include/logger.hrl").

-export([vhost_deleted/2, remove_leftovers/1]).

-spec vhost_deleted(rabbit_types:vhost(), vhost:id() | undefined) -> ok.
vhost_deleted(VHost, Id) ->
    case whereis(rabbit_mqtt_retainer_sup) of
        undefined -> ok;
        _ -> ok = rabbit_mqtt_retainer_sup:delete_child_for_vhost(VHost, Id)
    end,
    case rabbit_mqtt_retained_msg_store:delete_vhost(VHost, Id) of
        ok -> ok;
        {error, Reason} ->
            ?LOG_WARNING("Failed to delete MQTT retained messages of deleted vhost '~ts': ~tp",
                         [VHost, Reason])
    end.

-spec remove_leftovers(#{rabbit_types:vhost() => vhost:id() | undefined}) -> ok.
remove_leftovers(Live) ->
    case rabbit_mqtt_retained_msg_store:remove_leftovers(Live) of
        ok -> ok;
        {error, Reason} ->
            ?LOG_WARNING("Failed to remove MQTT retained messages of deleted vhosts: ~tp", [Reason])
    end.
