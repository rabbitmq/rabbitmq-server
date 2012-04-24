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
    OldUpstreams0 = children(Sup, ConnName),
    NewUpstreams0 = upstreams(XName, ConnName),
    %% If any haven't changed, don't restart them. The broker will
    %% avoid telling us about connections that have not changed
    %% syntactically, but even if one has, this X may not have that
    %% connection in an upstream, so we still need to check here.
    {OldUpstreams, NewUpstreams} =
        lists:foldl(
          fun (OldU, {OldUs, NewUs}) ->
                  case lists:member(OldU, NewUs) of
                      true  -> {OldUs -- [OldU], NewUs -- [OldU]};
                      false -> {OldUs, NewUs}
                  end
          end, {OldUpstreams0, NewUpstreams0}, OldUpstreams0),
    [stop(Sup, OldUpstream) || OldUpstream <- OldUpstreams],
    [start(Sup, NewUpstream, XName) || NewUpstream <- NewUpstreams];

adjust(Sup, XName, {clear_connection, ConnName}) ->
    prune_for_upstream_set(<<"all">>, XName),
    [stop(Sup, Upstream) || Upstream <- children(Sup, ConnName)];

%% TODO handle changes of upstream sets minimally (bug 24853)
adjust(Sup, XName, {upstream_set, Set}) ->
    prune_for_upstream_set(Set, XName),
    adjust(Sup, XName, everything);
adjust(Sup, XName, {clear_upstream_set, _}) ->
    adjust(Sup, XName, everything).

start(Sup, Upstream, XName) ->
    {ok, _Pid} = supervisor2:start_child(Sup, spec(Upstream, XName)),
    ok.

stop(Sup, Upstream) ->
    ok = supervisor2:terminate_child(Sup, Upstream),
    ok = supervisor2:delete_child(Sup, Upstream),
    %% While the link will report its own removal, that only works if
    %% the link was actually up. If the link was broken and failing to
    %% come up, the possibility exists that there *is* no link
    %% process, but we still have a report in the status table. So
    %% remove it here too.
    rabbit_federation_status:remove_upstream(Upstream).

children(Sup, ConnName) ->
    rabbit_federation_util:find_upstreams(
      ConnName, [U || {U, _, _, _} <- supervisor2:which_children(Sup)]).

upstreams(XName, ConnName) ->
    case upstream_set(XName) of
        {ok, UpstreamSet} ->
            rabbit_federation_upstream:from_set(UpstreamSet, XName, ConnName);
        {error, not_found} ->
            []
    end.

upstream_set(XName) ->
    %% TODO ahem
    <<"all">>.

prune_for_upstream_set(Set, XName) ->
    case upstream_set(XName) of
        {ok, Set} -> Us = rabbit_federation_upstream:from_set(Set, XName),
                     ok = rabbit_federation_db:prune_scratch(XName, Us);
        _         -> ok
    end.

%%----------------------------------------------------------------------------

init({UpstreamSet, X}) ->
    %% 1, 1 so that the supervisor can give up and get into waiting
    %% for the reconnect_delay quickly.
    {ok, {{one_for_one, 1, 1}, specs(UpstreamSet, X)}}.

specs(UpstreamSet, X) ->
    [spec(Upstream, X) ||
        Upstream <- rabbit_federation_upstream:from_set(UpstreamSet, X)].

spec(Upstream = #upstream{reconnect_delay = Delay}, #exchange{name = XName}) ->
    {Upstream, {rabbit_federation_link, start_link, [{Upstream, XName}]},
     {transient, Delay}, ?MAX_WAIT, worker,
     [rabbit_federation_link]}.
