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
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mqtt_collector).

-export([register/2, unregister/2, list/0, leave/1]).

%%----------------------------------------------------------------------------
register(ClientId, Pid) ->
    run_ra_command({register, ClientId, Pid}).

unregister(ClientId, Pid) ->
    run_ra_command({unregister, ClientId, Pid}).

list() ->
     run_ra_command(list).

leave(NodeBin) ->
    Node = binary_to_atom(NodeBin, utf8),
    run_ra_command({leave, Node}),
    mqtt_node:leave(Node).

%%----------------------------------------------------------------------------
-spec run_ra_command(term()) -> term() | {error, term()}.
run_ra_command(RaCommand) ->
    NodeId = mqtt_node:node_id(),
    case ra:process_command(NodeId, RaCommand) of
        {ok, Result, _} -> Result;
        _ = Error -> Error
    end.
