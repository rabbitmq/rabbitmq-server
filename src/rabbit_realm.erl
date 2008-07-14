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
%%   Portions created by LShift Ltd., Cohesive Financial Technologies
%%   LLC., and Rabbit Technologies Ltd. are Copyright (C) 2007-2008
%%   LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit
%%   Technologies Ltd.;
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_realm).

-export([recover/0]).
-export([add_realm/1, delete_realm/1, list_vhost_realms/1]).
-export([add/2, delete/2, check/2, delete_from_all/1]).
-export([access_request/3, enter_realm/3, leave_realms/1]).
-export([on_node_down/1]).

-include("rabbit.hrl").
-include_lib("stdlib/include/qlc.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(e_or_q() :: 'exchange' | 'queue').

-spec(recover/0 :: () -> 'ok').
-spec(add_realm/1 :: (realm_name()) -> 'ok').
-spec(delete_realm/1 :: (realm_name()) -> 'ok').
-spec(list_vhost_realms/1 :: (vhost()) -> [name()]).
-spec(add/2 :: (realm_name(), r(e_or_q())) ->  'ok').
-spec(delete/2 :: (realm_name(), r(e_or_q())) -> 'ok').
-spec(check/2 :: (realm_name(), r(e_or_q())) -> bool() | not_found()).
-spec(delete_from_all/1 :: (r(e_or_q())) -> 'ok').
-spec(access_request/3 :: (username(), bool(), ticket()) ->
             'ok' | not_found() | {'error', 'bad_realm_path' |
                                   'access_refused' | 
                                   'resource_locked'}).
-spec(enter_realm/3 :: (realm_name(), bool(), pid()) ->
             'ok' | {'error', 'resource_locked'}).
-spec(leave_realms/1 :: (pid()) -> 'ok').
-spec(on_node_down/1 :: (node()) -> 'ok').

-endif.

%%--------------------------------------------------------------------

recover() ->
    %% preens resource lists, limiting them to currently-extant resources
    rabbit_misc:execute_mnesia_transaction(fun preen_realms/0).

add_realm(Name = #resource{virtual_host = VHostPath, kind = realm}) ->
    rabbit_misc:execute_mnesia_transaction(
      rabbit_misc:with_vhost(
        VHostPath,
        fun () ->
                case mnesia:read({realm, Name}) of
                    [] ->
                        NewRealm = #realm{name = Name},
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
    rabbit_misc:execute_mnesia_transaction(
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

list_vhost_realms(VHostPath) ->
    [Name ||
        #vhost_realm{realm = #resource{name = Name}} <-
            %% TODO: use dirty ops instead
            rabbit_misc:execute_mnesia_transaction(
              rabbit_misc:with_vhost(
                VHostPath,
                fun () -> mnesia:read({vhost_realm, VHostPath}) end))].
        
add(Realm = #resource{kind = realm}, Resource = #resource{}) ->
    manage_link(fun mnesia:write/3, Realm, Resource).

delete(Realm = #resource{kind = realm}, Resource = #resource{}) ->
    manage_link(fun mnesia:delete_object/3, Realm, Resource).
    
% This links or unlinks a resource to a realm
manage_link(Action, Realm = #resource{kind = realm, name = RealmName}, 
            R = #resource{name = Name}) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              case mnesia:read({realm, Realm}) of
                  []  -> mnesia:abort(not_found);
                  [_] -> Action(realm_table_for_resource(R),
                                #realm_resource{realm = RealmName,
                                                resource = Name},
                                write)
              end
      end).
      
realm_table_for_resource(#resource{kind = exchange}) -> realm_exchange;
realm_table_for_resource(#resource{kind = queue})    -> realm_queue.
parent_table_for_resource(#resource{kind = exchange}) -> exchange;
parent_table_for_resource(#resource{kind = queue})    -> amqqueue.


check(#resource{kind = realm, name = Realm}, R = #resource{name = Name}) ->
    case mnesia:dirty_match_object(realm_table_for_resource(R),
                                   #realm_resource{realm = Realm,
                                                   resource = Name}) of
        [] -> false;
        _  -> true
    end.

% Requires a mnesia transaction.
delete_from_all(R = #resource{name = Name}) ->
    mnesia:delete_object(realm_table_for_resource(R),
                         #realm_resource{realm = '_', resource = Name},
                         write).

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
                            enter_realm(RealmName, Exclusive, self())
                    end
            end
    end.

enter_realm(Name = #resource{kind = realm}, IsExclusive, Pid) ->
    RealmVisitor = #realm_visitor{realm = Name, pid = Pid},
    rabbit_misc:execute_mnesia_transaction(
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
    rabbit_misc:execute_mnesia_transaction(
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

on_node_down(Node) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              lists:foreach(
                fun (T) -> ok = remove_visitors(Node, T) end,
                [exclusive_realm_visitor, realm_visitor]),
              ok
      end).

%%--------------------------------------------------------------------

%% This iterates through the realm_exchange and realm_queue link tables
%% and deletes rows that have no underlying exchange or queue record.
preen_realms() ->
    lists:foreach(fun preen_realm/1, [exchange, queue]),
    ok.

preen_realm(Kind) ->
    R = #resource{kind = Kind},
    Table = realm_table_for_resource(R),
    Cursor = qlc:cursor(
               qlc:q([L#realm_resource.resource ||
                         L <- mnesia:table(Table)])),
    preen_next(Cursor, Table, parent_table_for_resource(R)),
    qlc:delete_cursor(Cursor).

preen_next(Cursor, Table, ParentTable) ->
    case qlc:next_answers(Cursor, 1) of 
        [] -> ok;
        [Name] ->
            case mnesia:read({ParentTable, Name}) of
                [] -> mnesia:delete_object(
                        Table,
                        #realm_resource{realm = '_', resource = Name},
                        write);
                _  -> ok
            end,
            preen_next(Cursor, Table, ParentTable)
    end.    

check_and_lookup(RealmName = #resource{kind = realm,
                                       name = <<"/data", _/binary>>}) ->
    lookup(RealmName);
check_and_lookup(RealmName = #resource{kind = realm,
                                       name = <<"/admin", _/binary>>}) ->
    lookup(RealmName);
check_and_lookup(_) ->
    {error, bad_realm_path}.

lookup(Name = #resource{kind = realm}) ->
    rabbit_misc:dirty_read({realm, Name}).

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
        (not(TP) orelse PP) andalso
        (not(TA) orelse PA) andalso
        (not(TW) orelse PW) andalso
        (not(TR) orelse PR) ->
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
