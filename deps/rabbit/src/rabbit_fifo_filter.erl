%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

-module(rabbit_fifo_filter).

-export([eval/2]).

-spec eval(none |
           {property, rabbit_amqp_filter:filter_expressions()} |
           {jms, term()},
           #{atom() | binary() => atom() | binary() | number()}) ->
    boolean().
eval(none, _MsgMeta) ->
    %% A consumer without filter wants all messages.
    true;
eval({property, Expr}, MsgMeta) ->
    rabbit_fifo_filter_prop:eval(Expr, MsgMeta);
eval({jms, Expr}, MsgMeta) ->
    rabbit_fifo_filter_jms:eval(Expr, MsgMeta).
