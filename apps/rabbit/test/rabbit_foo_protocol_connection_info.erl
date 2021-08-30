%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2019-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(rabbit_foo_protocol_connection_info).

%% Dummy module to test authentication context propagation

%% API
-export([additional_authn_params/4]).

additional_authn_params(_Creds, _VHost, _Pid, Infos) ->
    case proplists:get_value(variable_map, Infos, undefined) of
        VariableMap when is_map(VariableMap) ->
            case maps:get(<<"key1">>, VariableMap, []) of
                Value when is_binary(Value)->
                    [{key1, Value}];
                [] ->
                    []
            end;
        _ ->
            []
    end.
