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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2018 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_ff_registry).

-export([list/1,
         is_supported/1,
         is_enabled/1]).

list(Arg) ->
    rabbit_feature_flags:initialize_registry(),
    ?MODULE:list(Arg).

is_supported(Arg) ->
    rabbit_feature_flags:initialize_registry(),
    ?MODULE:is_supported(Arg).

is_enabled(Arg) ->
    rabbit_feature_flags:initialize_registry(),
    ?MODULE:is_enabled(Arg).
