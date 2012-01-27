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
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_stomp).

-include("rabbit_stomp.hrl").

-behaviour(application).
-export([start/2, stop/1]).

-define(DEFAULT_CONFIGURATION,
        #stomp_configuration{
          default_login    = undefined,
          default_passcode = undefined,
          implicit_connect = false}).

start(normal, []) ->
    Config = parse_configuration(),
    Listeners = parse_listener_configuration(),
    rabbit_stomp_sup:start_link(Listeners, Config).

stop(_State) ->
    ok.

parse_listener_configuration() ->
    case application:get_env(tcp_listeners) of
        undefined ->
            throw({error, {stomp_configuration_not_found}});
        {ok, Listeners} ->
            case application:get_env(ssl_listeners) of
                undefined          -> {Listeners, []};
                {ok, SslListeners} -> {Listeners, SslListeners}
            end
    end.

parse_configuration() ->
    Configuration =
        case application:get_env(default_user) of
            undefined ->
                ?DEFAULT_CONFIGURATION;
            {ok, UserConfig} ->
                parse_default_user(UserConfig, ?DEFAULT_CONFIGURATION)
        end,
    report_configuration(Configuration),
    Configuration.

parse_default_user([], Configuration) ->
    Configuration;
parse_default_user([{login, Login} | Rest], Configuration) ->
    parse_default_user(Rest, Configuration#stomp_configuration{
                               default_login = Login});
parse_default_user([{passcode, Passcode} | Rest], Configuration) ->
    parse_default_user(Rest, Configuration#stomp_configuration{
                               default_passcode = Passcode});
parse_default_user([implicit_connect | Rest], Configuration) ->
    parse_default_user(Rest, Configuration#stomp_configuration{
                               implicit_connect = true});
parse_default_user([Unknown | Rest], Configuration) ->
    error_logger:error_msg("Invalid default_user configuration option: ~p~n",
                           [Unknown]),
    parse_default_user(Rest, Configuration).

report_configuration(#stomp_configuration{
                        default_login    = Login,
                        implicit_connect = ImplicitConnect}) ->
    case Login of
        undefined ->
            ok;
        _ ->
            error_logger:info_msg("Default user '~s' enabled~n", [Login])
    end,

    case ImplicitConnect of
        true  -> error_logger:info_msg("Implicit connect enabled~n");
        false -> ok
    end,

    ok.


