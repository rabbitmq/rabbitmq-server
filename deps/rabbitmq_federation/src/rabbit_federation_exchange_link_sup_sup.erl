%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_federation_exchange_link_sup_sup).

-behaviour(mirrored_supervisor).
-behaviour(rabbit_mnesia_to_khepri_record_converter).

-include_lib("rabbit/include/mirrored_supervisor.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-define(SUPERVISOR, ?MODULE).

%% Supervises the upstream links for all exchanges (but not queues). We need
%% different handling here since exchanges want a mirrored sup.

-export([start_link/0, start_child/1, adjust/1, stop_child/1]).
-export([init/1]).

%% Khepri paths don't support tuples or records, so the key part of the
%% #mirrored_sup_childspec{} used by some plugins must be  transformed in a
%% valid Khepri path during the migration from Mnesia to Khepri.
%% `rabbit_db_msup_m2k_converter` iterates over all declared converters, which
%% must implement `rabbit_mnesia_to_khepri_record_converter` behaviour callbacks.
%%
%% This mechanism could be reused by any other rabbit_db_*_m2k_converter

-rabbit_mnesia_records_to_khepri_db(
   [
    {rabbit_db_msup_m2k_converter, ?MODULE}
   ]).

-export([upgrade_record/2, upgrade_key/2]).

-spec upgrade_record(Table, Record) -> Record when
      Table :: mnesia_to_khepri:mnesia_table(),
      Record :: tuple().
upgrade_record(mirrored_sup_childspec,
               #mirrored_sup_childspec{key = {?MODULE, #exchange{} = Exchange}} = Record) ->
    Record#mirrored_sup_childspec{key = {?MODULE, id(Exchange)}};
upgrade_record(_Table, Record) ->
    Record.

-spec upgrade_key(Table, Key) -> Key when
      Table :: mnesia_to_khepri:mnesia_table(),
      Key :: any().
upgrade_key(mirrored_sup_childspec, {?MODULE, #exchange{} = Exchange}) ->
    {?MODULE, id(Exchange)};
upgrade_key(_Table, Key) ->
    Key.

%%----------------------------------------------------------------------------

start_link() ->
    _ = pg:start_link(),
    %% This scope is used by concurrently starting exchange and queue links,
    %% and other places, so we have to start it very early outside of the supervision tree.
    %% The scope is stopped in stop/1.
    _ = rabbit_federation_pg:start_scope(),
    mirrored_supervisor:start_link({local, ?SUPERVISOR}, ?SUPERVISOR,
                                   ?MODULE, []).

%% Note that the next supervisor down, rabbit_federation_link_sup, is common
%% between exchanges and queues.
start_child(X) ->
    Result =
        case child_exists(X) orelse
            mirrored_supervisor:start_child(
              ?SUPERVISOR,
              {id(X), {rabbit_federation_link_sup, start_link, [X]},
               transient, ?SUPERVISOR_WAIT, supervisor,
               [rabbit_federation_link_sup]}) of
            true ->
                already_started;
            {ok, _Pid} ->
                ok;
            {error, {already_started, _Pid}} ->
                already_started;
            %% A link returned {stop, gone}, the link_sup shut down, that's OK.
            {error, {shutdown, _}} ->
                ok
        end,
    case Result of
        ok ->
            ok;
        already_started ->
            #exchange{name = ExchangeName} = X,
            rabbit_log_federation:debug("Federation link for exchange ~tp was already started",
                                        [rabbit_misc:rs(ExchangeName)]),
            ok
    end.


child_exists(Name) ->
    Id = id(Name),
    %% older format, pre-3.13.0
    OldId = old_id(Name),
    lists:any(fun ({ChildId, _, _, _}) ->
                      ChildId =:= Id orelse ChildId =:= OldId
              end,
              mirrored_supervisor:which_children(?SUPERVISOR)).

adjust({clear_upstream, VHost, UpstreamName}) ->
    _ = [rabbit_federation_link_sup:adjust(Pid, exchange_record_from_child_id(Id), {clear_upstream, UpstreamName}) ||
            {Id, Pid, _, _} <- mirrored_supervisor:which_children(?SUPERVISOR),
            virtual_host_name_from_child_id(Id) =:= VHost
        ],
    ok;
adjust(Reason) ->
    _ = [rabbit_federation_link_sup:adjust(Pid, exchange_record_from_child_id(Id), Reason) ||
            {Id, Pid, _, _} <- mirrored_supervisor:which_children(?SUPERVISOR)
        ],
    ok.

stop_child(X) ->
    Result =
        case stop_and_delete_child(id(X)) of
            ok -> ok;
            {error, not_found} = Error ->
                case rabbit_khepri:is_enabled() of
                    true ->
                        %% Old id format is not supported by Khepri and cannot exist there
                        Error;
                    false ->
                        %% try old format, pre-3.13.0
                        stop_and_delete_child(old_id(X))
                end
        end,
    case Result of
        ok ->
            ok;
        {error, Err} ->
            #exchange{name = ExchangeName} = X,
            rabbit_log_federation:warning(
              "Attempt to stop a federation link for exchange ~tp failed: ~tp",
              [rabbit_misc:rs(ExchangeName), Err]),
            ok
    end.

stop_and_delete_child(Id) ->
    case mirrored_supervisor:terminate_child(?SUPERVISOR, Id) of
        ok ->
            ok = mirrored_supervisor:delete_child(?SUPERVISOR, Id);
        {error, not_found} = Error ->
            Error
    end.

%%----------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_one, 1200, 60}, []}}.

%% See comment in rabbit_federation_queue_link_sup_sup:id/1
id(X = #exchange{policy = Policy}) ->
    X1 = rabbit_exchange:immutable(X),
    {simple_id(X), X1#exchange{policy = Policy}}.

simple_id(#exchange{name = #resource{virtual_host = VHost, name = Name}}) ->
    [exchange, VHost, Name].

%% Old child id format, pre 3.13.0
old_id(X = #exchange{policy = Policy}) ->
    X1 = rabbit_exchange:immutable(X),
    X1#exchange{policy = Policy}.

%% New child id format, introduced in 3.13.0 for Khepri
exchange_record_from_child_id({_, #exchange{} = XR}) ->
    XR;
%% Old child id format, pre-3.13.0
exchange_record_from_child_id(#exchange{} = XR) ->
    XR.

virtual_host_name_from_child_id({_, #exchange{name = Res}}) ->
    Res#resource.virtual_host;
virtual_host_name_from_child_id(#exchange{name = Res}) ->
    Res#resource.virtual_host.
