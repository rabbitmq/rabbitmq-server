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
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_federation_exchange_link_sup_sup).

-behaviour(mirrored_supervisor).

-include_lib("rabbit_common/include/rabbit.hrl").
-define(SUPERVISOR, ?MODULE).

%% Supervises the upstream links for all exchanges (but not queues). We need
%% different handling here since exchanges want a mirrored sup.

-export([start_link/0, start_child/1, adjust/1, stop_child/1]).
-export([init/1]).

%%----------------------------------------------------------------------------

start_link() ->
    mirrored_supervisor:start_link({local, ?SUPERVISOR}, ?SUPERVISOR,
                                   fun rabbit_misc:execute_mnesia_transaction/1,
                                   ?MODULE, []).

%% Note that the next supervisor down, rabbit_federation_link_sup, is common
%% between exchanges and queues.
start_child(X) ->
    case mirrored_supervisor:start_child(
           ?SUPERVISOR,
           {id(X), {rabbit_federation_link_sup, start_link, [X]},
            transient, ?SUPERVISOR_WAIT, supervisor,
            [rabbit_federation_link_sup]}) of
        {ok, _Pid}               -> ok;
        {error, {already_started, _Pid}} ->
          #exchange{name = ExchangeName} = X,
          rabbit_log_federation:debug("Federation link for exchange ~p was already started",
                                      [rabbit_misc:rs(ExchangeName)]),
          ok;
        %% A link returned {stop, gone}, the link_sup shut down, that's OK.
        {error, {shutdown, _}} -> ok
    end.

adjust({clear_upstream, VHost, UpstreamName}) ->
    [rabbit_federation_link_sup:adjust(Pid, X, {clear_upstream, UpstreamName}) ||
        {#exchange{name = Name} = X, Pid, _, _} <- mirrored_supervisor:which_children(?SUPERVISOR),
        Name#resource.virtual_host == VHost],
    ok;
adjust(Reason) ->
    [rabbit_federation_link_sup:adjust(Pid, X, Reason) ||
        {X, Pid, _, _} <- mirrored_supervisor:which_children(?SUPERVISOR)],
    ok.

stop_child(X) ->
    case mirrored_supervisor:terminate_child(?SUPERVISOR, id(X)) of
      ok -> ok;
      {error, Err} ->
        #exchange{name = ExchangeName} = X,
        rabbit_log_federation:warning(
          "Attempt to stop a federation link for exchange ~p failed: ~p",
          [rabbit_misc:rs(ExchangeName), Err]),
        ok
    end,
    ok = mirrored_supervisor:delete_child(?SUPERVISOR, id(X)).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_one, 1200, 60}, []}}.

%% See comment in rabbit_federation_queue_link_sup_sup:id/1
id(X = #exchange{policy = Policy}) -> X1 = rabbit_exchange:immutable(X),
                                      X1#exchange{policy = Policy}.
