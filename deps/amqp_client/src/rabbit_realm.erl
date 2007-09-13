%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial
%%   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C) 
%%   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit 
%%   Technologies Ltd.; 
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_realm).

-export([start_link/0]).

-export([recover/0]).
-export([add_realm/1, delete_realm/1, lookup/1, list_vhost_realms/1]).
-export([add/3, delete/3, check/3, delete_from_all/2]).
-export([access_request/3, enter_realm/3, leave_realms/1, exit_realms/1]).
-export([on_node_down/1]).

-include("rabbit.hrl").
-include_lib("stdlib/include/qlc.hrl").

%%--------------------------------------------------------------------

start_link() ->
    rabbit_monitor:start_link({local, rabbit_realm_monitor},
                              {?MODULE, leave_realms}).

recover() ->
    %% preens resource lists, limiting them to currently-extant resources
    {atomic, ok} =
        mnesia:transaction
          (fun () ->
                   Realms = mnesia:foldl(fun preen_realm/2, [], realm),
                   lists:foreach(fun mnesia:write/1, Realms),
                   ok
           end),
    ok.

add_realm(Name = #resource{virtual_host = VHostPath, kind = realm}) ->
    rabbit_misc:execute_simple_mnesia_transaction(
      rabbit_misc:with_vhost(
        VHostPath,
        fun () ->
                case mnesia:read({realm, Name}) of
                    [] ->
                        NewRealm = #realm{name = Name,
                                          exchanges = ordsets:new(),
                                          queues = ordsets:new()},
                        ok = mnesia:write(NewRealm),
                        ok = mnesia:write(
                               #vhost_realm{virtual_host = VHostPath,
                                            realm = Name}),
                        ok;
                    [_R] ->
                        mnesia:abort({realm_already_exists, Name})
                end
        end)).

delete_realm(Name = #resource{virtual_host = VHostPath, kind = realm}) ->
    rabbit_misc:execute_simple_mnesia_transaction(
      rabbit_misc:with_vhost(
        VHostPath,
        rabbit_misc:with_realm(
          Name,
          fun () ->
                  ok = mnesia:delete({realm, Name}),
                  ok = mnesia:delete_object(
                         #vhost_realm{virtual_host = VHostPath,
                                      realm = Name}),
                  lists:foreach(fun mnesia:delete_object/1,
                                mnesia:index_read(user_realm, Name,
                                                  #user_realm.realm)),
                  ok
          end))).

lookup(Name = #resource{kind = realm}) ->
    rabbit_misc:clean_read({realm, Name}).

list_vhost_realms(VHostPath) ->
    rabbit_misc:execute_mnesia_transaction(
      rabbit_misc:with_vhost(
        VHostPath,
        fun () ->
                mnesia:read({vhost_realm, VHostPath})
        end),
      fun (Result) ->
              {ok, lists:map(fun (#vhost_realm{realm = #resource{name = N}}) ->
                                     N
                             end,
                             Result)}
      end).

add(Name = #resource{kind = realm}, What, Resource) ->
    internal_update_realm_byname(Name, What, Resource,
                                 fun ordsets:add_element/2).

delete(Name = #resource{kind = realm}, What, Resource) ->
    internal_update_realm_byname(Name, What, Resource,
                                 fun ordsets:del_element/2).

check(Name = #resource{kind = realm}, What, Resource) ->
    case rabbit_misc:clean_read({realm, Name}) of
        {ok, R} ->
            case What of
                exchange -> ordsets:is_element(Resource, R#realm.exchanges);
                queue -> ordsets:is_element(Resource, R#realm.queues)
            end;
        Other -> Other
    end.

% Requires a mnesia transaction.
delete_from_all(What, Resource) ->
    Realms = mnesia:foldl
               (fun (Realm = #realm{exchanges = E0,
                                    queues = Q0},
                     Acc) ->
                        IsMember = lists:member(Resource,
                                                case What of
                                                    exchange -> E0;
                                                    queue -> Q0
                                                end),
                        if
                            IsMember ->
                                [internal_update_realm_record(
                                   Realm, What, Resource,
                                   fun ordsets:del_element/2)
                                 | Acc];
                            true ->
                                Acc
                        end
                end, [], realm),
    lists:foreach(fun mnesia:write/1, Realms),
    ok.

access_request(Username, Exclusive, Ticket = #ticket{realm_name = RealmName})
  when is_binary(Username) ->
    %% FIXME: We should do this all in a single tx. Otherwise we may
    %% a) get weird answers, b) create inconsistencies in the db
    %% (e.g. realm_visitor records referring to non-existing realms).
    case check_and_lookup(RealmName) of
        {error, Reason} ->
            {error, Reason};
        {ok, _Realm} ->
            {ok, U} = rabbit_access_control:lookup_user(Username),
            case rabbit_access_control:lookup_realm_access(U, RealmName) of
                none ->
                    {error, access_refused};
                TicketPattern ->
                    case match_ticket(TicketPattern, Ticket) of
                        no_match ->
                            {error, access_refused};
                        match ->
                            case enter_realm(RealmName, Exclusive, self()) of
                                ok ->
                                    rabbit_monitor:monitor(
                                      rabbit_realm_monitor, self()),
                                    ok;
                                Other -> Other
                            end
                    end
            end
    end.

enter_realm(Name = #resource{kind = realm}, IsExclusive, Pid) ->
    RealmVisitor = #realm_visitor{realm = Name, pid = Pid},
    rabbit_misc:execute_simple_mnesia_transaction(
      fun () ->
              case mnesia:read({exclusive_realm_visitor, Name}) of
                  [] when IsExclusive ->
                      ok = mnesia:delete_object(RealmVisitor),
                      %% TODO: find a more efficient way of checking
                      %% for "no machting results" that doesn't
                      %% involve retrieving all the records
                      case mnesia:read({realm_visitor, Name}) of
                          [] ->
                              mnesia:write(
                                exclusive_realm_visitor, RealmVisitor, write),
                              ok;
                          [_|_] ->
                              {error, resource_locked}
                      end;
                  [] ->
                      ok = mnesia:write(RealmVisitor),
                      ok;
                  [RealmVisitor] when IsExclusive -> ok;
                  [RealmVisitor] ->
                      ok = mnesia:delete({exclusive_realm_visitor, Name}),
                      ok = mnesia:write(RealmVisitor),
                      ok;
                  [_] ->
                      {error, resource_locked}
              end
      end).

leave_realms(Pid) ->
    rabbit_misc:execute_simple_mnesia_transaction(
      fun () ->
              case mnesia:index_read(exclusive_realm_visitor, Pid,
                                     #realm_visitor.pid) of
                  [] -> ok;
                  [R] ->
                      ok = mnesia:delete_object(
                             exclusive_realm_visitor, R, write)
              end,
              lists:foreach(fun mnesia:delete_object/1,
                            mnesia:index_read(realm_visitor, Pid,
                                              #realm_visitor.pid)),
              ok
      end).

exit_realms(Pid) ->
    rabbit_monitor:fake_exit(rabbit_realm_monitor, Pid).

on_node_down(Node) ->
    rabbit_misc:execute_simple_mnesia_transaction(
      fun () ->
              lists:foreach(
                fun (T) -> ok = remove_visitors(Node, T) end,
                [exclusive_realm_visitor, realm_visitor]),
              ok
      end).

%%--------------------------------------------------------------------

preen_realm(Realm = #realm{name = #resource{kind = realm},
                           exchanges = E0,
                           queues = Q0},
            Realms) ->
    [Realm#realm{exchanges = filter_out_missing(E0, exchange),
                 queues = filter_out_missing(Q0, amqqueue)}
     | Realms].

filter_out_missing(Items, TableName) ->
    ordsets:filter(fun (Item) ->
                           case mnesia:read({TableName, Item}) of
                               [] -> false;
                               _ -> true
                           end
                   end, Items).

internal_update_realm_byname(Name, What, Resource, SetUpdater) ->
    rabbit_misc:execute_simple_mnesia_transaction(
      fun () ->
              case mnesia:read({realm, Name}) of
                  [] ->
                      mnesia:abort(not_found);
                  [R] ->
                      ok = mnesia:write(internal_update_realm_record
                                        (R, What, Resource, SetUpdater))
              end
      end).

internal_update_realm_record(R = #realm{exchanges = E0, queues = Q0},
                             What, Resource, SetUpdater) ->
    case What of
        exchange -> R#realm{exchanges = SetUpdater(Resource, E0)};
        queue -> R#realm{queues = SetUpdater(Resource, Q0)}
    end.

check_and_lookup(RealmName = #resource{kind = realm,
                                       name = <<"/data", _/binary>>}) ->
    lookup(RealmName);
check_and_lookup(RealmName = #resource{kind = realm,
                                       name = <<"/admin", _/binary>>}) ->
    lookup(RealmName);
check_and_lookup(_) ->
    {error, bad_realm_path}.

match_ticket(#ticket{passive_flag = PP,
                     active_flag  = PA,
                     write_flag   = PW,
                     read_flag    = PR},
             #ticket{passive_flag = TP,
                     active_flag  = TA,
                     write_flag   = TW,
                     read_flag    = TR}) ->
    if
        %% Matches if either we're not requesting passive access, or
        %% passive access is permitted, and ...
        %%
        %% We want to write
        %%        (not(TP) orelse PP) andalso
        %%        (not(TA) orelse PA) andalso
        %%        (not(TW) orelse PW) andalso
        %%        (not(TR) orelse PR) ->
        %% but this trips up dialyzer. This has been reported to the
        %% developers and they will fix that bug in the next release.
        (TP =:= false orelse PP) andalso
        (TA =:= false orelse PA) andalso
        (TW =:= false orelse PW) andalso
        (TR =:= false orelse PR) ->
            match;
        true ->
            no_match
    end.

remove_visitors(Node, T) ->
    qlc:fold(
      fun (R, Acc) ->
              ok = mnesia:delete_object(T, R, write),
              Acc
      end,
      ok,
      qlc:q([R || R = #realm_visitor{pid = Pid} <- mnesia:table(T),
                  node(Pid) == Node])).
