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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_unit_test).

-define(INFO, [{<<"baz">>, longstr, <<"bam">>}]).
-define(H, <<"x-forwarding">>).

-include_lib("eunit/include/eunit.hrl").

routing_test() ->
    ?assertEqual([{<<"x-forwarding">>, array, [{table, ?INFO}]}],
                 add(undefined)),

    ?assertEqual([{<<"x-forwarding">>, array, [{table, ?INFO}]}],
                 add([])),

    ?assertEqual([{<<"foo">>, longstr, <<"bar">>},
                  {<<"x-forwarding">>, array, [{table, ?INFO}]}],
                 add([{<<"foo">>, longstr, <<"bar">>}])),

    ?assertEqual([{<<"x-forwarding">>, array, [{table, ?INFO},
                                               {table, ?INFO}]}],
                 add([{<<"x-forwarding">>, array, [{table, ?INFO}]}])),
    ok.

add(Table) ->
    rabbit_federation_link:add_routing_to_headers(Table, ?INFO).
