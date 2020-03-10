%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2019-2020 VMware, Inc. or its affiliates.  All rights reserved.
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