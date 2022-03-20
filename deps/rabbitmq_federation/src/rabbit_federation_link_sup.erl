%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_federation_link_sup).

-behaviour(supervisor2).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit/include/amqqueue.hrl").
-include("rabbit_federation.hrl").

%% Supervises the upstream links for an exchange or queue.

-export([start_link/1, adjust/3, restart/2]).
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

adjust(Sup, X = #exchange{name = XName}, {upstream_set, _Set}) ->
    adjust(Sup, X, everything),
    case rabbit_federation_upstream:federate(X) of
        false -> ok;
        true  -> ok = rabbit_federation_db:prune_scratch(
                        XName, rabbit_federation_upstream:for(X))
    end;
adjust(Sup, Q, {upstream_set, _}) when ?is_amqqueue(Q) ->
    adjust(Sup, Q, everything);
adjust(Sup, XorQ, {clear_upstream_set, _}) ->
    adjust(Sup, XorQ, everything).

restart(Sup, Upstream) ->
    ok = supervisor2:terminate_child(Sup, Upstream),
    {ok, _Pid} = supervisor2:restart_child(Sup, Upstream),
    ok.

start(Sup, Upstream, XorQ) ->
    {ok, _Pid} = supervisor2:start_child(Sup, spec(rabbit_federation_util:obfuscate_upstream(Upstream), XorQ)),
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
    %% 1, ?MAX_WAIT so that we always give up after one fast retry and get
    %% into the reconnect delay.
    {ok, {{one_for_one, 1, ?MAX_WAIT}, specs(XorQ)}}.

specs(XorQ) ->
    [spec(rabbit_federation_util:obfuscate_upstream(Upstream), XorQ)
     || Upstream <- rabbit_federation_upstream:for(XorQ)].

spec(U = #upstream{reconnect_delay = Delay}, #exchange{name = XName}) ->
    {U, {rabbit_federation_exchange_link, start_link, [{U, XName}]},
     {permanent, Delay}, ?WORKER_WAIT, worker,
     [rabbit_federation_exchange_link]};

spec(Upstream = #upstream{reconnect_delay = Delay}, Q) when ?is_amqqueue(Q) ->
    {Upstream, {rabbit_federation_queue_link, start_link, [{Upstream, Q}]},
     {permanent, Delay}, ?WORKER_WAIT, worker,
     [rabbit_federation_queue_link]}.

name(#exchange{name = XName}) -> XName;
name(Q) when ?is_amqqueue(Q) -> amqqueue:get_name(Q).
