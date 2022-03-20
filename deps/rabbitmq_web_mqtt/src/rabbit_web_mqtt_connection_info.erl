%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_web_mqtt_connection_info).

%% Module to add the MQTT client ID to authentication properties

%% API
-export([additional_authn_params/4]).

additional_authn_params(_Creds, _VHost, _Pid, Infos) ->
    case proplists:get_value(variable_map, Infos, undefined) of
        VariableMap when is_map(VariableMap) ->
            case maps:get(<<"client_id">>, VariableMap, []) of
                ClientId when is_binary(ClientId)->
                    [{client_id, ClientId}];
                [] ->
                    []
            end;
        _ ->
            []
    end.
