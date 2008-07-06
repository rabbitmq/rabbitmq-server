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

-module(rabbit_ticket).
-include("rabbit.hrl").

-export([record_ticket/2, lookup_ticket/4, check_ticket/4]).

-import(application).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(ticket_number() :: non_neg_integer()).
%% we'd like to write #ticket.passive_flag | #ticket.active_flag | ...
%% but dialyzer doesn't support that.
-type(ticket_field() :: 3..6).

-spec(record_ticket/2 :: (ticket_number(), ticket()) -> 'ok').
-spec(lookup_ticket/4 ::
      (ticket_number(), ticket_field(), username(), vhost()) ->
             ticket()).
-spec(check_ticket/4 ::
      (ticket_number(), ticket_field(), r('exchange' | 'queue'), username()) ->
             'ok').

-endif.

%%----------------------------------------------------------------------------

record_ticket(TicketNumber, Ticket) ->
    put({ticket, TicketNumber}, Ticket),
    ok.

lookup_ticket(TicketNumber, FieldIndex, Username, VHostPath) ->
    case get({ticket, TicketNumber}) of
        undefined ->
            %% Spec: "The server MUST isolate access tickets per
            %% channel and treat an attempt by a client to mix these
            %% as a connection exception."
            rabbit_log:warning("Attempt by client to use invalid ticket ~p~n", [TicketNumber]),
            maybe_relax_checks(TicketNumber, Username, VHostPath);
        Ticket = #ticket{} ->
            case element(FieldIndex, Ticket) of
                false -> rabbit_misc:protocol_error(
                           access_refused,
                           "ticket ~w has insufficient permissions",
                           [TicketNumber]);
                true  -> Ticket
            end
    end.

maybe_relax_checks(TicketNumber, Username, VHostPath) ->
    case rabbit_misc:strict_ticket_checking() of
        true ->
            rabbit_misc:protocol_error(
              access_refused, "invalid ticket ~w", [TicketNumber]);
        false ->
            rabbit_log:warning("Lax ticket check mode: fabricating full ticket ~p for user ~p, vhost ~p~n",
                               [TicketNumber, Username, VHostPath]),
            Ticket = rabbit_access_control:full_ticket(
                       rabbit_misc:r(VHostPath, realm, <<"/data">>)),
            case rabbit_realm:access_request(Username, false, Ticket) of
                ok -> record_ticket(TicketNumber, Ticket),
                      Ticket;
                {error, Reason} ->
                    rabbit_misc:protocol_error(
                      Reason,
                      "fabrication of ticket ~w for user '~s' in vhost '~s' failed",
                      [TicketNumber, Username, VHostPath])
            end
    end.

check_ticket(TicketNumber, FieldIndex,
             Name = #resource{virtual_host = VHostPath}, Username) ->
    #ticket{realm_name = RealmName} =
        lookup_ticket(TicketNumber, FieldIndex, Username, VHostPath),
    case resource_in_realm(RealmName, Name) of
                false ->
                    case rabbit_misc:strict_ticket_checking() of
                        true ->
                            rabbit_misc:protocol_error(
                              access_refused,
                              "insufficient permissions in ticket ~w to access ~s in ~s",
                              [TicketNumber, rabbit_misc:rs(Name),
                               rabbit_misc:rs(RealmName)]);
                        false ->
                            rabbit_log:warning("Lax ticket check mode: ignoring cross-realm access for ticket ~p~n", [TicketNumber]),
                            ok
                    end;
                true ->
                    ok
            end.

resource_in_realm(RealmName, ResourceName = #resource{kind = Kind}) ->
    CacheKey = {resource_cache, RealmName, Kind},
    case get(CacheKey) of
        Name when Name == ResourceName ->
            true;
        _ ->
            case rabbit_realm:check(RealmName, ResourceName) of
                true ->
                    put(CacheKey, ResourceName),
                    true;
                _ ->
                    false
            end
    end.
