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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_federation_link_sup).

-behaviour(supervisor2).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("rabbit_federation.hrl").

%% Supervises the upstream links for an exchange or queue.

-export([start_link/1, adjust/3]).
-export([init/1]).

start_link(XorQ) ->
    supervisor2:start_link(?MODULE, XorQ).

adjust(Sup, XorQ, everything) ->
    [stop(Sup, Upstream, XorQ) ||
        {Upstream, _, _, _} <- supervisor2:which_children(Sup)],
    [{ok, _Pid} = supervisor2:start_child(Sup, Spec) || Spec <- specs(XorQ)];

adjust(Sup, XorQ, {upstream, UpstreamName}) ->
    OldUpstreams0 = children(Sup, UpstreamName),
    NewUpstreams0 = rabbit_federation_upstream:for(XorQ, UpstreamName),
    %% If any haven't changed, don't restart them. The broker will
    %% avoid telling us about connections that have not changed
    %% syntactically, but even if one has, this XorQ may not have that
    %% connection in an upstream, so we still need to check here.
    {OldUpstreams, NewUpstreams} =
        lists:foldl(
          fun (OldU, {OldUs, NewUs}) ->
                  case lists:member(OldU, NewUs) of
                      true  -> {OldUs -- [OldU], NewUs -- [OldU]};
                      false -> {OldUs, NewUs}
                  end
          end, {OldUpstreams0, NewUpstreams0}, OldUpstreams0),
    [stop(Sup, OldUpstream, XorQ) || OldUpstream <- OldUpstreams],
    [start(Sup, NewUpstream, XorQ) || NewUpstream <- NewUpstreams];

adjust(Sup, XorQ, {clear_upstream, UpstreamName}) ->
    ok = rabbit_federation_db:prune_scratch(
           name(XorQ), rabbit_federation_upstream:for(XorQ)),
    [stop(Sup, Upstream, XorQ) || Upstream <- children(Sup, UpstreamName)];

%% TODO handle changes of upstream sets minimally (bug 24853)
adjust(Sup, X = #exchange{name = XName}, {upstream_set, Set}) ->
    case rabbit_federation_upstream:federate(X) of
        false -> ok;
        true  -> ok = rabbit_federation_db:prune_scratch(
                        XName, rabbit_federation_upstream:for(X))
    end,
    adjust(Sup, X, everything);
adjust(Sup, Q = #amqqueue{}, {upstream_set, _}) ->
    adjust(Sup, Q, everything);
adjust(Sup, XorQ, {clear_upstream_set, _}) ->
    adjust(Sup, XorQ, everything).

start(Sup, Upstream, XorQ) ->
    {ok, _Pid} = supervisor2:start_child(Sup, spec(Upstream, XorQ)),
    ok.

stop(Sup, Upstream, XorQ) ->
    ok = supervisor2:terminate_child(Sup, Upstream),
    ok = supervisor2:delete_child(Sup, Upstream),
    %% While the link will report its own removal, that only works if
    %% the link was actually up. If the link was broken and failing to
    %% come up, the possibility exists that there *is* no link
    %% process, but we still have a report in the status table. So
    %% remove it here too.
    rabbit_federation_status:remove(Upstream, name(XorQ)).

children(Sup, UpstreamName) ->
    rabbit_federation_util:find_upstreams(
      UpstreamName, [U || {U, _, _, _} <- supervisor2:which_children(Sup)]).

%%----------------------------------------------------------------------------

init(XorQ) ->
    %% 1, 1 so that the supervisor can give up and get into waiting
    %% for the reconnect_delay quickly.
    {ok, {{one_for_one, 1, 1}, specs(XorQ)}}.

specs(XorQ) ->
    [spec(Upstream, XorQ) || Upstream <- rabbit_federation_upstream:for(XorQ)].

spec(U = #upstream{reconnect_delay = Delay}, #exchange{name = XName}) ->
    {U, {rabbit_federation_exchange_link, start_link, [{U, XName}]},
     {permanent, Delay}, ?MAX_WAIT, worker,
     [rabbit_federation_link]};

spec(Upstream = #upstream{reconnect_delay = Delay}, Q = #amqqueue{}) ->
    {Upstream, {rabbit_federation_queue_link, start_link, [{Upstream, Q}]},
     {permanent, Delay}, ?MAX_WAIT, worker,
     [rabbit_federation_queue_link]}.

name(#exchange{name = XName}) -> XName;
name(#amqqueue{name = QName}) -> QName.
