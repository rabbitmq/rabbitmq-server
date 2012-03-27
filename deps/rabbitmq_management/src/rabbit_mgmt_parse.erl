%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Management Plugin.
%%
%%   The Initial Developer of the Original Code is VMware, Inc.
%%   Copyright (c) 2010-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_parse).

-export([parameter_value/1]).

%% TODO there is a bunch of stuff in format / util that should be moved here.
%% The idea is format means to JSON, this module means from JSON

parameter_value({struct, Props}) ->
    [{K, parameter_value(V)} || {K, V} <- Props];

parameter_value(L) when is_list(L) ->
    [parameter_value(V) || V <- L];

parameter_value(N) when is_number(N) -> N;
parameter_value(B) when is_binary(B) -> B;
parameter_value(null) ->                null;
parameter_value(true) ->                true;
parameter_value(false) ->               false.
