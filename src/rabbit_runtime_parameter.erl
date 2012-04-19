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

-module(rabbit_runtime_parameter).

-ifdef(use_specs).

-type(validate_results() ::
        'ok' | {error, string(), [term()]} | [validate_results()]).

-callback validate(binary(), binary(), term()) -> validate_results().
-callback validate_clear(binary(), binary()) -> validate_results().
-callback notify(binary(), binary(), term()) -> 'ok'.
-callback notify_clear(binary(), binary()) -> 'ok'.

-else.

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [
     {validate, 3},
     {validate_clear, 2},
     {notify, 3},
     {notify_clear, 2}
    ];
behaviour_info(_Other) ->
    undefined.

-endif.
