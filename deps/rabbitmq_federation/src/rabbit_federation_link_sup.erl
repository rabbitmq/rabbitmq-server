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
%% The Original Code is RabbitMQ Federation.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_link_sup).

-behaviour(supervisor2).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("rabbit_federation.hrl").

%% Supervises the upstream links for an exchange.

-export([start_link/1, adjust/3]).
-export([init/1]).

start_link(Args) -> supervisor2:start_link(?MODULE, Args).

adjust(Sup, _XName, everything) ->
    [restart(Sup, Upstream) ||
        {Upstream, _, _, _} <- supervisor2:which_children(Sup)],
    ok;

adjust(Sup, XName, {connection, ConnName}) ->
    case child(Sup, ConnName) of
        {ok, Upstream}     -> restart(Sup, Upstream);
        {error, not_found} -> start(Sup, XName, ConnName)
    end;

adjust(Sup, _XName, {clear_connection, ConnName}) ->
    case child(Sup, ConnName) of
        {ok, Upstream}     -> stop(Sup, Upstream);
        {error, not_found} -> ok
    end;

%% TODO handle changes of upstream sets properly
adjust(Sup, XName, {upstream_set, _}) ->
    adjust(Sup, XName, everything);
adjust(Sup, XName, {clear_upstream_set, _}) ->
    adjust(Sup, XName, everything).

start(Sup, XName, ConnName) ->
    case rabbit_exchange:lookup(XName) of
        {ok, #exchange{arguments = Args}} ->
            {longstr, UpstreamSetName} =
                rabbit_misc:table_lookup(Args, <<"upstream-set">>),
            case rabbit_federation_upstream:from_set(
                   UpstreamSetName, XName, ConnName) of
                {ok, Upstream} ->
                    {ok, _Pid} = supervisor2:start_child(
                                   Sup, spec(Upstream, XName)),
                    ok;
                {error, not_found} ->
                    ok
            end;
        {error, not_found} ->
            ok
    end.

restart(Sup, Upstream) ->
    ok = supervisor2:terminate_child(Sup, Upstream),
    {ok, _Pid} = supervisor2:restart_child(Sup, Upstream).

stop(Sup, Upstream) ->
    ok = supervisor2:terminate_child(Sup, Upstream),
    ok = supervisor2:delete_child(Sup, Upstream).

child(Sup, ConnName) ->
    rabbit_federation_util:find_upstream(
      ConnName, [U || {U, _, _, _} <- supervisor2:which_children(Sup)]).

%%----------------------------------------------------------------------------

init({UpstreamSet, XName}) ->
    %% We can't look this up in fed_exchange or fed_sup since
    %% mirrored_sup may fail us over to a different node with a
    %% different definition of the same upstream-set. This is the
    %% first point at which we know we're not switching nodes.
    {ok, Upstreams} = rabbit_federation_upstream:from_set(UpstreamSet, XName),
    %% 1, 1 so that the supervisor can give up and get into waiting
    %% for the reconnect_delay quickly.
    {ok, {{one_for_one, 1, 1},
          [spec(Upstream, XName) || Upstream <- Upstreams]}}.

spec(Upstream = #upstream{reconnect_delay = Delay}, XName) ->
    {Upstream, {rabbit_federation_link, start_link, [{Upstream, XName}]},
     {transient, Delay}, ?MAX_WAIT, worker,
     [rabbit_federation_link]}.
