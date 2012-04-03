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

adjust(Sup, XName, everything) ->
    [stop(Sup, Upstream) ||
        {Upstream, _, _, _} <- supervisor2:which_children(Sup)],
    case upstream_set(XName) of
        {ok, UpstreamSet} ->
            [{ok, _Pid} = supervisor2:start_child(Sup, Spec) ||
                Spec <- specs(UpstreamSet, XName)];
        {error, not_found} ->
            ok
    end;

adjust(Sup, XName, {connection, ConnName}) ->
    maybe_stop(Sup, ConnName),
    start(Sup, XName, ConnName);

adjust(Sup, _XName, {clear_connection, ConnName}) ->
    maybe_stop(Sup, ConnName);

%% TODO handle changes of upstream sets properly
adjust(Sup, XName, {upstream_set, _}) ->
    adjust(Sup, XName, everything);
adjust(Sup, XName, {clear_upstream_set, _}) ->
    adjust(Sup, XName, everything).

start(Sup, XName, ConnName) ->
    case upstream_set(XName) of
        {ok, UpstreamSet} ->
            case rabbit_federation_upstream:from_set(
                   UpstreamSet, XName, ConnName) of
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

maybe_stop(Sup, ConnName) ->
    case child(Sup, ConnName) of
        {ok, Upstream} ->
            stop(Sup, Upstream),
            %% While the link will report its own removal, that only
            %% works if the link was actually up. If the link was
            %% broken and failing to come up, the possibility exists
            %% that there *is* no link process, but we still have a
            %% report in the status table. So remove it here too.
            rabbit_federation_status:remove_upstream(Upstream);
        {error, not_found} ->
            ok
    end.

stop(Sup, Upstream) ->
    ok = supervisor2:terminate_child(Sup, Upstream),
    ok = supervisor2:delete_child(Sup, Upstream).

child(Sup, ConnName) ->
    rabbit_federation_util:find_upstream(
      ConnName, [U || {U, _, _, _} <- supervisor2:which_children(Sup)]).

upstream_set(XName) ->
    case rabbit_exchange:lookup(XName) of
        {ok, #exchange{arguments = Args}} ->
            {longstr, UpstreamSet} =
                rabbit_misc:table_lookup(Args, <<"upstream-set">>),
            {ok, UpstreamSet};
        {error, not_found} ->
            {error, not_found}
    end.

%%----------------------------------------------------------------------------

init({UpstreamSet, XName}) ->
    %% 1, 1 so that the supervisor can give up and get into waiting
    %% for the reconnect_delay quickly.
    {ok, {{one_for_one, 1, 1}, specs(UpstreamSet, XName)}}.

specs(UpstreamSet, XName) ->
    {ok, Upstreams} = rabbit_federation_upstream:from_set(UpstreamSet, XName),
    [spec(Upstream, XName) || Upstream <- Upstreams].

spec(Upstream = #upstream{reconnect_delay = Delay}, XName) ->
    {Upstream, {rabbit_federation_link, start_link, [{Upstream, XName}]},
     {transient, Delay}, ?MAX_WAIT, worker,
     [rabbit_federation_link]}.
