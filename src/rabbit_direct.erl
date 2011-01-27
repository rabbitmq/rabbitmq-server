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
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_direct).

-export([boot/0, connect/3, start_channel/5]).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(boot/0 :: () -> 'ok').
-spec(connect/3 :: (binary(), binary(), binary()) ->
                       {'ok', {rabbit_types:user(),
                               rabbit_framing:amqp_table()}}).
-spec(start_channel/5 :: (rabbit_channel:channel_number(), pid(),
                          rabbit_types:user(), rabbit_types:vhost(), pid()) ->
                             {'ok', pid()}).

-endif.

%%----------------------------------------------------------------------------

boot() ->
    {ok, _} =
        supervisor2:start_child(
            rabbit_sup,
            {rabbit_direct_client_sup,
             {rabbit_client_sup, start_link,
              [{local, rabbit_direct_client_sup},
               {rabbit_channel_sup, start_link, []}]},
             transient, infinity, supervisor, [rabbit_client_sup]}),
    ok.

%%----------------------------------------------------------------------------

connect(Username, Password, VHost) ->
    case lists:keymember(rabbit, 1, application:which_applications()) of
        true  ->
            try rabbit_access_control:user_pass_login(Username, Password) of
                #user{} = User ->
                    try rabbit_access_control:check_vhost_access(User, VHost) of
                        ok -> {ok, {User, rabbit_reader:server_properties()}}
                    catch
                        exit:#amqp_error{name = access_refused} ->
                            {error, access_refused}
                    end
            catch
                exit:#amqp_error{name = access_refused} -> {error, auth_failure}
            end;
        false ->
            {error, broker_not_found_on_node}
    end.

start_channel(Number, ClientChannelPid, User, VHost, Collector) ->
    {ok, _, {ChannelPid, _}} =
        supervisor2:start_child(
            rabbit_direct_client_sup,
            [{direct, Number, ClientChannelPid, User, VHost, Collector}]),
    {ok, ChannelPid}.
