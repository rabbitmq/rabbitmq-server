%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ Management Plugin.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2019 Pivotal Software, Inc.  All rights reserved.
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
    {[{<<"application/json">>, accept_content}], ReqData, Context}.

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
