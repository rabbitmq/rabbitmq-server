%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

-module(rabbit_amqp_filter).

-export([eval/2]).

-type expression() :: undefined |
                      {property, rabbit_amqp_filter_prop:parsed_expressions()} |
                      {jms, rabbit_amqp_filter_jms:parsed_expression()}.

-export_type([expression/0]).

-spec eval(expression(), mc:state()) -> boolean().
eval(undefined, _Mc) ->
    %% A receiver without filter wants all messages.
    true;
eval({property, Expr}, Mc) ->
    rabbit_amqp_filter_prop:eval(Expr, Mc);
eval({jms, Expr}, Mc) ->
    rabbit_amqp_filter_jms:eval(Expr, Mc).
