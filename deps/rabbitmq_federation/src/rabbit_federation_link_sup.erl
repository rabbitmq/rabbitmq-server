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
%% Copyright (c) 2007-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_link_sup).

-behaviour(supervisor2).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("rabbit_federation.hrl").

%% Supervises the upstream links for an exchange.

-export([start_link/1, adjust/3]).
-export([init/1]).

start_link(X) ->
    supervisor2:start_link(?MODULE, X).

adjust(Sup, X, everything) ->
    [stop(Sup, Upstream, X) ||
        {Upstream, _, _, _} <- supervisor2:which_children(Sup)],
    [{ok, _Pid} = supervisor2:start_child(Sup, Spec) || Spec <- specs(X)];

adjust(Sup, X, {upstream, UpstreamName}) ->
    OldUpstreams0 = children(Sup, UpstreamName),
    NewUpstreams0 = rabbit_federation_upstream:for(X, UpstreamName),
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
    [stop(Sup, OldUpstream, X) || OldUpstream <- OldUpstreams],
    [start(Sup, NewUpstream, X) || NewUpstream <- NewUpstreams];

adjust(Sup, X = #exchange{name = XName}, {clear_upstream, UpstreamName}) ->
    ok = rabbit_federation_db:prune_scratch(
           XName, rabbit_federation_upstream:for(X)),
    [stop(Sup, Upstream, X) || Upstream <- children(Sup, UpstreamName)];

%% TODO handle changes of upstream sets minimally (bug 24853)
adjust(Sup, X = #exchange{name = XName}, {upstream_set, Set}) ->
    case rabbit_federation_upstream:set_for(X) of
        {ok, Set} -> ok = rabbit_federation_db:prune_scratch(
                            XName, rabbit_federation_upstream:for(X));
        _         -> ok
    end,
    adjust(Sup, X, everything);
adjust(Sup, X, {clear_upstream_set, _}) ->
    adjust(Sup, X, everything).

start(Sup, Upstream, X) ->
    {ok, _Pid} = supervisor2:start_child(Sup, spec(Upstream, X)),
    ok.

stop(Sup, Upstream, #exchange{name = XName}) ->
    ok = supervisor2:terminate_child(Sup, Upstream),
    ok = supervisor2:delete_child(Sup, Upstream),
    %% While the link will report its own removal, that only works if
    %% the link was actually up. If the link was broken and failing to
    %% come up, the possibility exists that there *is* no link
    %% process, but we still have a report in the status table. So
    %% remove it here too.
    rabbit_federation_status:remove(Upstream, XName).

children(Sup, UpstreamName) ->
    rabbit_federation_util:find_upstreams(
      UpstreamName, [U || {U, _, _, _} <- supervisor2:which_children(Sup)]).

%%----------------------------------------------------------------------------

init(X) ->
    %% 1, 1 so that the supervisor can give up and get into waiting
    %% for the reconnect_delay quickly.
    {ok, {{one_for_one, 1, 1}, specs(X)}}.

specs(X) ->
    [spec(Upstream, X) || Upstream <- rabbit_federation_upstream:for(X)].

spec(Upstream = #upstream{reconnect_delay = Delay}, #exchange{name = XName}) ->
    {Upstream, {rabbit_federation_link, start_link, [{Upstream, XName}]},
     {permanent, Delay}, ?MAX_WAIT, worker,
     [rabbit_federation_link]}.
