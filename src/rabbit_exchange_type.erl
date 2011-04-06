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
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_exchange_type).

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [
     {description, 0},
     {route, 2},

     %% called BEFORE declaration, to check args etc; may exit with #amqp_error{}
     {validate, 1},

     %% called after declaration when previously absent
     {create, 2},

     %% called when recovering
     {recover, 2},

     %% called after exchange deletion.
     {delete, 3},

     %% called after a binding has been added
     {add_binding, 3},

     %% called after bindings have been deleted.
     {remove_bindings, 3},

     %% called when comparing exchanges for equivalence - should return ok or
     %% exit with #amqp_error{}
     {assert_args_equivalence, 2}

    ];
behaviour_info(_Other) ->
    undefined.
