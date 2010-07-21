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
%%   The Original Code is the RabbitMQ Erlang Client.
%%
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial
%%   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C)
%%   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit
%%   Technologies Ltd.;
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ____________________.

%% @private
-module(amqp_channel_sup).

-include("amqp_client.hrl").

-behaviour(supervisor).

-export([start_child/2]).
-export([init/1]).

%%---------------------------------------------------------------------------

start_child(SupRef, ChildSpec) ->
    case supervisor:start_child(SupRef, ChildSpec) of
        {ok, Child} when is_pid(Child) ->
            Child;
        {error, Reason} ->
            erlang:error({could_not_start_channel_infrastructure, Reason})
    end.

%%---------------------------------------------------------------------------
%% supervisor callbacks
%%---------------------------------------------------------------------------

init(ChannelArgs) ->
    ChChildSpec =
        {amqp_channel,
         {amqp_channel, start_link, [{self(), ChannelArgs}]},
         transient, ?MAX_WAIT, worker, [gen_server]},
    {ok, {{one_for_all, 0, 1}, [ChChildSpec]}}.
