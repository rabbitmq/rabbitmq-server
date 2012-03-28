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
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_exchange_decorator).

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [
     {description, 0},

     %% Should Rabbit ensure that all binding events that are
     %% delivered to an individual exchange can be serialised? (they
     %% might still be delivered out of order, but there'll be a
     %% serial number).
     {serialise_events, 0},

     {route, 2},

     %% called after declaration and recovery
     {create, 2},

     %% called after exchange (auto)deletion.
     {delete, 3},

     %% called after a binding has been added or recovered
     {add_binding, 3},

     %% called after bindings have been deleted.
     {remove_bindings, 3}
    ];
behaviour_info(_Other) ->
    undefined.
