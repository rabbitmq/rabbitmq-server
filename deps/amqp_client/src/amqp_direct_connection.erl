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
%%   Contributor(s): Ben Hood <0x6e6562@gmail.com>.

%% @private
-module(amqp_direct_connection).

-include("amqp_client.hrl").

-behaviour(amqp_gen_connection).

-export([init/1, terminate/2, connect/3, do/2, open_channel_args/1, i/2,
         info_keys/0, handle_message/2, closing_state_set/3,
         channels_terminated/1]).

-record(state, {user,
                vhost,
                collector,
                closing_reason = false %% false | Reason
               }).

-define(INFO_KEYS, [type]).

%%---------------------------------------------------------------------------

init([]) ->
    {ok, #state{}}.

open_channel_args(#state{user = User, vhost = VHost, collector = Collector}) ->
    [User, VHost, Collector].

do(_Method, _State) ->
    ok.

handle_message(Msg, State) ->
    {stop, {unexpected_msg, Msg}, State}.

closing_state_set(_ChannelCloseType, Reason, State) ->
    {ok, State#state{closing_reason = Reason}}.

channels_terminated(State = #state{closing_reason = Reason,
                                   collector = Collector}) ->
    rabbit_queue_collector:delete_all(Collector),
    {stop, Reason, State}.

terminate(_Reason, _State) ->
    ok.

i(type, _State) -> direct;
i(Item, _State) -> throw({bad_argument, Item}).

info_keys() ->
    ?INFO_KEYS.

connect(AmqpParams, SIF, State) ->
    try do_connect(AmqpParams, SIF, State) of
        Return -> Return
    catch
        exit:#amqp_error{name = access_refused} -> {error, auth_failure};
        _:Reason                                -> {error, Reason}
    end.

do_connect(#amqp_params{username = User, password = Pass, virtual_host = VHost},
           SIF, State) ->
    case lists:keymember(rabbit, 1, application:which_applications()) of
        true  -> rabbit_access_control:user_pass_login(User, Pass),
                 rabbit_access_control:check_vhost_access(
                         #user{username = User, password = Pass}, VHost),
                 {ok, {ChMgr, Collector}} = SIF(),
                 {ok, rabbit_reader:server_properties(), 0, ChMgr,
                  State#state{user = User,
                              vhost = VHost,
                              collector = Collector}};
        false -> {error, broker_not_found_in_vm}
    end.
