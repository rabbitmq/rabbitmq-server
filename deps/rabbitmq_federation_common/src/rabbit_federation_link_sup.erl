%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_federation_link_sup).

-behaviour(supervisor2).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit/include/amqqueue.hrl").
-include("rabbit_federation.hrl").

%% Supervises the upstream links for an exchange or queue.

-export([start_link/2, adjust/4, restart/2]).
-export([init/1]).

start_link(LinkMod, Q) ->
    supervisor2:start_link(?MODULE, [LinkMod, Q]).

adjust(Sup, LinkMod, XorQ, everything) ->
    _ = [stop(Sup, Upstream, XorQ) ||
        {Upstream, _, _, _} <- supervisor2:which_children(Sup)],
    [{ok, _Pid} = supervisor2:start_child(Sup, Spec)
     || Spec <- specs(LinkMod, XorQ)];

adjust(Sup, LinkMod, XorQ, {upstream, UpstreamName}) ->
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
    _ = [stop(Sup, OldUpstream, XorQ) || OldUpstream <- OldUpstreams],
    [start(Sup, LinkMod, NewUpstream, XorQ) || NewUpstream <- NewUpstreams];

adjust(Sup, _LinkMod, XorQ, {clear_upstream, UpstreamName}) ->
    ok = rabbit_federation_db:prune_scratch(
           name(XorQ), rabbit_federation_upstream:for(XorQ)),
    [stop(Sup, Upstream, XorQ) || Upstream <- children(Sup, UpstreamName)];

adjust(Sup, LinkMod, X = #exchange{name = XName}, {upstream_set, _Set}) ->
    _ = adjust(Sup, LinkMod, X, everything),
    case rabbit_federation_upstream:federate(X) of
        false -> ok;
        true  -> ok = rabbit_federation_db:prune_scratch(
                        XName, rabbit_federation_upstream:for(X))
    end;
adjust(Sup, LinkMod, Q, {upstream_set, _}) when ?is_amqqueue(Q) ->
    adjust(Sup, LinkMod, Q, everything);
adjust(Sup, LinkMod, XorQ, {clear_upstream_set, _}) ->
    adjust(Sup, LinkMod, XorQ, everything).

restart(Sup, Upstream) ->
    ok = supervisor2:terminate_child(Sup, Upstream),
    {ok, _Pid} = supervisor2:restart_child(Sup, Upstream),
    ok.

start(Sup, LinkMod, Upstream, XorQ) ->
    {ok, _Pid} = supervisor2:start_child(Sup, spec(LinkMod, rabbit_federation_util:obfuscate_upstream(Upstream), XorQ)),
    ok.

stop(Sup, Upstream, XorQ) ->
    ok = supervisor2:terminate_child(Sup, Upstream),
    ok = supervisor2:delete_child(Sup, Upstream),
    %% While the link will report its own removal, that only works if
    %% the link was actually up. If the link was broken and failing to
    %% come up, the possibility exists that there *is* no link
    %% process, but we still have a report in the status table. So
    %% remove it here too.
    %% TODO how do we figure out the module without adding a dependency?
    rabbit_federation_status:remove(Upstream, name(XorQ)).

children(Sup, UpstreamName) ->
    rabbit_federation_util:find_upstreams(
      UpstreamName, [U || {U, _, _, _} <- supervisor2:which_children(Sup)]).

%%----------------------------------------------------------------------------

init([LinkMod, XorQ]) ->
    %% 1, ?MAX_WAIT so that we always give up after one fast retry and get
    %% into the reconnect delay.
    {ok, {{one_for_one, 1, ?MAX_WAIT}, specs(LinkMod, XorQ)}}.

specs(LinkMod, XorQ) ->
    [spec(LinkMod, rabbit_federation_util:obfuscate_upstream(Upstream), XorQ)
     || Upstream <- rabbit_federation_upstream:for(XorQ)].

spec(LinkMod, U = #upstream{reconnect_delay = Delay}, #exchange{name = XName}) ->
    {U, {LinkMod, start_link, [{U, XName}]},
     {transient, Delay}, ?WORKER_WAIT, worker,
     [LinkMod]};

spec(LinkMod, Upstream = #upstream{reconnect_delay = Delay}, Q) when ?is_amqqueue(Q) ->
    {Upstream, {LinkMod, start_link, [{Upstream, Q}]},
     {transient, Delay}, ?WORKER_WAIT, worker,
     [LinkMod]}.

name(#exchange{name = XName}) -> XName;
name(Q) when ?is_amqqueue(Q) -> amqqueue:get_name(Q).
