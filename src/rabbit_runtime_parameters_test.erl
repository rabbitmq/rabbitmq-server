%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_runtime_parameters_test).
-behaviour(rabbit_runtime_parameter).

-export([validate/3, validate_clear/2, notify/3, notify_clear/2]).
-export([register/0, unregister/0]).

register() ->
    rabbit_registry:register(runtime_parameter, <<"test">>, ?MODULE).

unregister() ->
    rabbit_registry:unregister(runtime_parameter, <<"test">>).

validate(<<"test">>, <<"good">>,  _Term)      -> ok;
validate(<<"test">>, <<"maybe">>, <<"good">>) -> ok;
validate(<<"test">>, _, _)                    -> {error, "meh", []}.

validate_clear(<<"test">>, <<"good">>)  -> ok;
validate_clear(<<"test">>, <<"maybe">>) -> ok;
validate_clear(<<"test">>, _)           -> {error, "meh", []}.

notify(_, _, _) -> ok.
notify_clear(_, _) -> ok.
