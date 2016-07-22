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
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_connection_tracking_handler).

%% This module keeps track of connection creation and termination events
%% on its local node. The primary goal here is to decouple connection
%% tracking from rabbit_reader in rabbit_common.
%%
%% Events from other nodes are ignored.

%% This module keeps track of connection creation and termination events
%% on its local node. The primary goal here is to decouple connection
%% tracking from rabbit_reader in rabbit_common.
%%
%% Events from other nodes are ignored.

-behaviour(gen_event).

-export([init/1, handle_call/2, handle_event/2, handle_info/2,
         terminate/2, code_change/3]).

-include_lib("rabbit.hrl").

-rabbit_boot_step({?MODULE,
                   [{description, "connection tracking event handler"},
                    {mfa,         {gen_event, add_handler,
                                   [rabbit_event, ?MODULE, []]}},
                    {cleanup,     {gen_event, delete_handler,
                                   [rabbit_event, ?MODULE, []]}},
                    {requires,    [rabbit_event, rabbit_node_monitor]},
                    {enables,     recovery}]}).


%%
%% API
%%

init([]) ->
    {ok, []}.

handle_event(#event{type = connection_created, props = Details}, State) ->
    rabbit_connection_tracking:register_connection(
        rabbit_connection_tracking:tracked_connection_from_connection_created(Details)
    ),
    {ok, State};
%% see rabbit_reader
handle_event(#event{type = connection_reregistered, props = [{state, ConnState}]}, State) ->
    rabbit_connection_tracking:register_connection(
        rabbit_connection_tracking:tracked_connection_from_connection_state(ConnState)
    ),
    {ok, State};
handle_event(#event{type = connection_closed, props = Details}, State) ->
    %% [{name,<<"127.0.0.1:64078 -> 127.0.0.1:5672">>},
    %%  {pid,<0.1774.0>},
    %%  {node, rabbit@hostname}]
    rabbit_connection_tracking:unregister_connection(
        {proplists:get_value(node, Details),
         proplists:get_value(name, Details)}),
    {ok, State};
handle_event(#event{type = vhost_deleted, props = Details}, State) ->
    VHost = proplists:get_value(name, Details),
    rabbit_log_connection:info("Closing all connections in vhost '~s' because it's being deleted", [VHost]),
    case rabbit_connection_tracking:list(VHost) of
      [] -> {ok, State};
      Cs ->
        [rabbit_networking:close_connection(Pid, rabbit_misc:format("vhost '~s' is deleted", [VHost])) || #tracked_connection{pid = Pid} <- Cs],
        {ok, State}
    end;
handle_event(#event{type = user_deleted, props = Details}, State) ->
    _Username = proplists:get_value(name, Details),
    %% TODO: force close and unregister connections from
    %%       this user. Moved to rabbitmq/rabbitmq-server#628.
    {ok, State};
handle_event(_Event, State) ->
    {ok, State}.

handle_call(_Request, State) ->
    {ok, not_understood, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Arg, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
