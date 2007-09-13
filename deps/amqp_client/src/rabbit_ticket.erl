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

-module(rabbit_ticket).
-include("rabbit.hrl").

-export([record_ticket/2, lookup_ticket/5, check_ticket/5]).

-import(application).

record_ticket(TicketNumber, Ticket) ->
    put({ticket, TicketNumber}, Ticket).

lookup_ticket(TicketNumber, FieldIndex, Username, VHostPath, Method) ->
    case get({ticket, TicketNumber}) of
        undefined ->
            %% Spec: "The server MUST isolate access tickets per
            %% channel and treat an attempt by a client to mix these
            %% as a connection exception."
            rabbit_log:warning("Attempt by client to use invalid ticket ~p~n", [TicketNumber]),
            maybe_relax_checks(TicketNumber, Username, VHostPath, Method);
        Ticket = #ticket{} ->
            case element(FieldIndex, Ticket) of
                false ->
                    rabbit_misc:die(access_refused, Method);
                true ->
                    {ok, Ticket}
            end
    end.

maybe_relax_checks(TicketNumber, Username, VHostPath, Method) ->
    case rabbit_misc:strict_ticket_checking() of
        true ->
            rabbit_misc:die(access_refused, Method);
        false ->
            rabbit_log:warning("Lax ticket check mode: fabricating full ticket number ~p~n",
                               [TicketNumber]),
            Ticket = rabbit_access_control:full_ticket(
                       rabbit_misc:r(VHostPath, realm, <<"/data">>)),
            case rabbit_realm:access_request(Username, false, Ticket) of
                ok -> record_ticket(TicketNumber, Ticket),
                      {ok, Ticket};
                {error, Reason} ->
                    rabbit_log:info("Ticket fabrication for User ~p, VHost ~p, Realm ~p failed: ~p~n",
                                    [Username, VHostPath, "/data", Reason]),
                    rabbit_misc:die(Reason, Method)
            end
    end.

check_ticket(TicketNumber, FieldIndex,
             Name = #resource{virtual_host = VHostPath}, Username,
             Method) ->
    {ok, Ticket} = lookup_ticket(TicketNumber, FieldIndex,
                                 Username, VHostPath, Method),
    case resource_in_realm(Ticket#ticket.realm_name, Name) of
        false ->
            case rabbit_misc:strict_ticket_checking() of
                true ->
                    rabbit_misc:die(access_refused, Method);
                false ->
                    rabbit_log:warning("Lax ticket check mode: ignoring cross-realm access for ticket ~p~n", [TicketNumber]),
                    {ok, Ticket}
            end;
        true ->
            {ok, Ticket}
    end.

resource_in_realm(RealmName, ResourceName = #resource{kind = Kind}) ->
    CacheKey = {resource_cache, RealmName, Kind},
    case get(CacheKey) of
        Name when Name == ResourceName ->
            true;
        _ ->
            case rabbit_realm:check(RealmName, Kind, ResourceName) of
                true ->
                    put(CacheKey, ResourceName),
                    true;
                _ ->
                    false
            end
    end.
