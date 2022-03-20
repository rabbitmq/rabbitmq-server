%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2019-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_feature_flag_enable).

-export([init/2,
         content_types_accepted/2, is_authorized/2,
         allowed_methods/2, accept_content/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(Req, _Args) ->
    {cowboy_rest,
     rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE),
     #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_accepted(ReqData, Context) ->
    {[{{<<"application">>, <<"json">>, '*'}, accept_content}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"PUT">>, <<"OPTIONS">>], ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).

accept_content(ReqData, #context{} = Context) ->
    NameS = rabbit_mgmt_util:id(name, ReqData),
    try
        Name = list_to_existing_atom(binary_to_list(NameS)),
        case rabbit_feature_flags:enable(Name) of
            ok ->
                {true, ReqData, Context};
            {error, Reason1} ->
                FormattedReason1 = rabbit_ff_extra:format_error(Reason1),
                rabbit_mgmt_util:bad_request(
                  list_to_binary(FormattedReason1), ReqData, Context)
        end
    catch
        _:badarg ->
            Reason2 = unsupported,
            FormattedReason2 = rabbit_ff_extra:format_error(Reason2),
            rabbit_mgmt_util:bad_request(
              list_to_binary(FormattedReason2), ReqData, Context)
    end.
