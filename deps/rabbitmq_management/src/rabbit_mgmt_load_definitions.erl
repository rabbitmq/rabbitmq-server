%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ Management Plugin.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_load_definitions).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([maybe_load_definitions/0, maybe_load_definitions_from/2]).

maybe_load_definitions() ->
    rabbit_definitions:maybe_load_definitions().

maybe_load_definitions_from(true, Dir) ->
    rabbit_definitions:maybe_load_definitions_from(true, Dir);
maybe_load_definitions_from(false, File) ->
    rabbit_definitions:maybe_load_definitions_from(false, File).
