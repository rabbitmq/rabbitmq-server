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
-module(amqp_channel_sup_sup).

-include("amqp_client.hrl").

-behaviour(supervisor).

-export([start_link/0, start_channel_sup/4]).
-export([init/1]).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

start_link() ->
    supervisor:start_link(?MODULE, none).

start_channel_sup(Sup, ChannelNumber, Driver, InfraArgs) ->
    ChildSpec =
        {name(ChannelNumber),
         {amqp_channel_sup, start_link, [ChannelNumber, Driver, InfraArgs]},
         temporary, infinity, supervisor, [amqp_infra_sup]},
    supervisor:start_child(Sup, ChildSpec).

%%---------------------------------------------------------------------------
%% supervisor callbacks
%%---------------------------------------------------------------------------

init(_Args) ->
    {ok, {{one_for_one, 0, 1}, []}}.

%%---------------------------------------------------------------------------
%% Internal plumbing
%%---------------------------------------------------------------------------

name(ChannelNumber) ->
    list_to_atom("ch" ++ integer_to_list(ChannelNumber) ++ "_sup_" ++ uuid()).

uuid() ->
    {A, B, C} = now(),
    erlang:integer_to_list(A, 16) ++ "-" ++
        erlang:integer_to_list(B, 16) ++ "-" ++
        erlang:integer_to_list(C, 16).
