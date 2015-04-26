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
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_mqtt_retained_msg_store).

-export([behaviour_info/1, table_name_for/1]).

behaviour_info(callbacks) ->
    [{new,       2},
     {recover,   2},
     {insert,    3},
     {lookup,    2},
     {delete,    2},
     {terminate, 1}];
behaviour_info(_Other) ->
    undefined.

table_name_for(VHost) ->
  rabbit_mqtt_util:vhost_name_to_table_name(VHost).
