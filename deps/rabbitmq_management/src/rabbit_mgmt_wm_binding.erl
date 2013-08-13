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
%%   The Original Code is RabbitMQ Management Plugin.
%%
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2010-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_wm_binding).

-export([init/1, resource_exists/2, to_json/2,
         content_types_provided/2, content_types_accepted/2,
         is_authorized/2, allowed_methods/2, delete_resource/2,
         args_hash/1]).

-include("rabbit_mgmt.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%%--------------------------------------------------------------------
init(_Config) -> {ok, #context{}}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

content_types_accepted(ReqData, Context) ->
   {[{"application/json", accept_content}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {['HEAD', 'GET', 'DELETE'], ReqData, Context}.

resource_exists(ReqData, Context) ->
    Binding = binding(ReqData),
    {case Binding of
         not_found        -> false;
         {bad_request, _} -> false;
         _                -> case rabbit_binding:exists(Binding) of
                                 true -> true;
                                 _    -> false
                             end
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    with_binding(ReqData, Context,
                 fun(Binding) ->
                         rabbit_mgmt_util:reply(
                           rabbit_mgmt_format:binding(Binding),
                           ReqData, Context)
                 end).

delete_resource(ReqData, Context) ->
    MethodName = case rabbit_mgmt_util:destination_type(ReqData) of
                     exchange -> 'exchange.unbind';
                     queue    -> 'queue.unbind'
                 end,
    sync_resource(MethodName, ReqData, Context).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_vhost(ReqData, Context).

%%--------------------------------------------------------------------

binding(ReqData) ->
    case rabbit_mgmt_util:vhost(ReqData) of
        not_found -> not_found;
        VHost     -> Source = rabbit_mgmt_util:id(source, ReqData),
                     Dest = rabbit_mgmt_util:id(destination, ReqData),
                     DestType = rabbit_mgmt_util:destination_type(ReqData),
                     Props = rabbit_mgmt_util:id(props, ReqData),
                     SName = rabbit_misc:r(VHost, exchange, Source),
                     DName = rabbit_misc:r(VHost, DestType, Dest),
                     case unpack(SName, DName, Props) of
                         {bad_request, Str} ->
                             {bad_request, Str};
                         {Key, Args} ->
                             #binding{ source      = SName,
                                       destination = DName,
                                       key         = Key,
                                       args        = Args }
                     end
    end.

unpack(Src, Dst, Props) ->
    case rabbit_mgmt_format:tokenise(binary_to_list(Props)) of
        ["~"]          -> {<<>>, []};
        [Key]          -> {unquote(Key), []};
        ["~", ArgsEnc] -> lookup(<<>>, ArgsEnc, Src, Dst);
        [Key, ArgsEnc] -> lookup(unquote(Key), ArgsEnc, Src, Dst);
        _              -> {bad_request, {too_many_tokens, Props}}
    end.

lookup(RoutingKey, ArgsEnc, Src, Dst) ->
    lookup(RoutingKey, unquote(ArgsEnc),
           rabbit_binding:list_for_source_and_destination(Src, Dst)).

lookup(_RoutingKey, _Hash, []) ->
    {bad_request, "binding not found"};
lookup(RoutingKey, Hash, [#binding{args = Args} | Rest]) ->
    case args_hash(Args) =:= Hash of
        true  -> {RoutingKey, Args};
        false -> lookup(RoutingKey, Hash, Rest)
    end.

args_hash(Args) ->
    list_to_binary(rabbit_misc:base64url(erlang:md5(term_to_binary(Args)))).

unquote(Name) ->
    list_to_binary(mochiweb_util:unquote(Name)).

with_binding(ReqData, Context, Fun) ->
    case binding(ReqData) of
        {bad_request, Reason} ->
            rabbit_mgmt_util:bad_request(Reason, ReqData, Context);
        Binding ->
            Fun(Binding)
    end.

sync_resource(MethodName, ReqData, Context) ->
    with_binding(
      ReqData, Context,
      fun(Binding) ->
              Props0 = rabbit_mgmt_format:binding(Binding),
              Props = Props0 ++
                  [{exchange, proplists:get_value(source,      Props0)},
                   {queue,    proplists:get_value(destination, Props0)}],
              rabbit_mgmt_util:amqp_request(
                rabbit_mgmt_util:vhost(ReqData), ReqData, Context,
                rabbit_mgmt_util:props_to_method(MethodName, Props))
      end).
