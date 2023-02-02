%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_msup).

-export([
         create_tables/0,
         table_definitions/0,
         create_or_update/5,
         find_mirror/2,
         update_all/2,
         delete/2,
         delete_all/1
        ]).

-export([clear/0]).

-define(TABLE, mirrored_sup_childspec).
-define(TABLE_DEF,
        {?TABLE,
         [{record_name, mirrored_sup_childspec},
          {type, ordered_set},
          {attributes, record_info(fields, mirrored_sup_childspec)}]}).
-define(TABLE_MATCH, {match, #mirrored_sup_childspec{ _ = '_' }}).

-record(mirrored_sup_childspec, {key, mirroring_pid, childspec}).

%% -------------------------------------------------------------------
%% create_tables().
%% -------------------------------------------------------------------

-spec create_tables() -> Ret when
      Ret :: 'ok' | {error, Reason :: term()}.

create_tables() ->
    rabbit_db:run(
      #{mnesia => fun() -> create_tables_in_mnesia([?TABLE_DEF]) end
       }).

create_tables_in_mnesia([]) ->
    ok;
create_tables_in_mnesia([{Table, Attributes} | Ts]) ->
    case mnesia:create_table(Table, Attributes) of
        {atomic, ok}                        -> create_tables_in_mnesia(Ts);
        {aborted, {already_exists, ?TABLE}} -> create_tables_in_mnesia(Ts);
        Err                                 -> Err
    end.

%% -------------------------------------------------------------------
%% table_definitions().
%% -------------------------------------------------------------------

-spec table_definitions() -> [Def] when
      Def :: {Name :: atom(), term()}.

table_definitions() ->
    {Name, Attributes} = ?TABLE_DEF,
    [{Name, [?TABLE_MATCH | Attributes]}].

%% -------------------------------------------------------------------
%% create_or_update().
%% -------------------------------------------------------------------

-spec create_or_update(Group, Overall, Delegate, ChildSpec, Id) -> Ret when
      Group :: any(),
      Overall :: pid(),
      Delegate :: pid() | undefined,
      ChildSpec :: supervisor2:child_spec(),
      Id :: {any(), any()},
      Ret :: start | undefined | pid().

create_or_update(Group, Overall, Delegate, ChildSpec, Id) ->
    rabbit_db:run(
      #{mnesia =>
            fun() ->
                    create_or_update_in_mnesia(Group, Overall, Delegate, ChildSpec, Id)
            end}).

create_or_update_in_mnesia(Group, Overall, Delegate, ChildSpec, Id) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              ReadResult = mnesia:wread({?TABLE, {Group, Id}}),
              rabbit_log:debug("Mirrored supervisor: check_start table ~ts read for key ~tp returned ~tp",
                               [?TABLE, {Group, Id}, ReadResult]),
              case ReadResult of
                  []  -> _ = write_in_mnesia(Group, Overall, ChildSpec, Id),
                         start;
                  [S] -> #mirrored_sup_childspec{key           = {Group, Id},
                                                 mirroring_pid = Pid} = S,
                         case Overall of
                             Pid ->
                                 rabbit_log:debug("Mirrored supervisor: overall matched mirrored pid ~tp", [Pid]),
                                 Delegate;
                             _   ->
                                 rabbit_log:debug("Mirrored supervisor: overall ~tp did not match mirrored pid ~tp", [Overall, Pid]),
                                 Sup = mirrored_supervisor:supervisor(Pid),
                                 rabbit_log:debug("Mirrored supervisor: supervisor(~tp) returned ~tp", [Pid, Sup]),
                                 case Sup of
                                     dead      ->
                                         _ = write_in_mnesia(Group, Overall, ChildSpec, Id),
                                         start;
                                     Delegate0 ->
                                         Delegate0
                                 end
                         end
              end
      end).

write_in_mnesia(Group, Overall, ChildSpec, Id) ->
    S = #mirrored_sup_childspec{key           = {Group, Id},
                                mirroring_pid = Overall,
                                childspec     = ChildSpec},
    ok = mnesia:write(?TABLE, S, write),
    ChildSpec.

%% -------------------------------------------------------------------
%% delete().
%% -------------------------------------------------------------------

-spec delete(Group, Id) -> ok when
      Group :: any(),
      Id :: any().

delete(Group, Id) ->
    rabbit_db:run(
      #{mnesia => fun() -> delete_in_mnesia(Group, Id) end
       }).

delete_in_mnesia(Group, Id) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              ok = mnesia:delete({?TABLE, {Group, Id}})
      end).

%% -------------------------------------------------------------------
%% find_mirror().
%% -------------------------------------------------------------------

-spec find_mirror(Group, Id) -> Ret when
      Group :: any(),
      Id :: any(),
      Ret :: {ok, pid()} | {error, not_found}.

find_mirror(Group, Id) ->
    %% If we did this inside a tx we could still have failover
    %% immediately after the tx - we can't be 100% here. So we may as
    %% well dirty_select.
    rabbit_db:run(
      #{mnesia => fun() -> find_mirror_in_mnesia(Group, Id) end
       }).

find_mirror_in_mnesia(Group, Id) ->
    MatchHead = #mirrored_sup_childspec{mirroring_pid = '$1',
                                        key           = {Group, Id},
                                        _             = '_'},
    case mnesia:dirty_select(?TABLE, [{MatchHead, [], ['$1']}]) of
        [Mirror] -> {ok, Mirror};
        _ -> {error, not_found}
    end.

%% -------------------------------------------------------------------
%% update_all().
%% -------------------------------------------------------------------

-spec update_all(Overall, Overall) -> [ChildSpec] when
      Overall :: pid(),
      ChildSpec :: supervisor2:child_spec().

update_all(Overall, OldOverall) ->
    rabbit_db:run(
      #{mnesia => fun() -> update_all_in_mnesia(Overall, OldOverall) end
       }).

update_all_in_mnesia(Overall, OldOverall) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              MatchHead = #mirrored_sup_childspec{mirroring_pid = OldOverall,
                                                  key           = '$1',
                                                  childspec     = '$2',
                                                  _             = '_'},
              [write_in_mnesia(Group, Overall, C, Id) ||
                  [{Group, Id}, C] <- mnesia:select(?TABLE, [{MatchHead, [], ['$$']}])]
      end).

%% -------------------------------------------------------------------
%% delete_all().
%% -------------------------------------------------------------------

-spec delete_all(Group) -> ok when
      Group :: any().

delete_all(Group) ->
    rabbit_db:run(
      #{mnesia => fun() -> delete_all_in_mnesia(Group) end
       }).

delete_all_in_mnesia(Group) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              MatchHead = #mirrored_sup_childspec{key       = {Group, '$1'},
                                                  _         = '_'},
              [ok = mnesia:delete({?TABLE, {Group, Id}}) ||
                  Id <- mnesia:select(?TABLE, [{MatchHead, [], ['$1']}])]
      end),
    ok.

%% -------------------------------------------------------------------
%% clear().
%% -------------------------------------------------------------------

-spec clear() -> ok.

clear() ->
    rabbit_db:run(
      #{mnesia => fun() -> clear_in_mnesia() end}).

clear_in_mnesia() ->
    {atomic, ok} = mnesia:clear_table(?TABLE),
    ok.
