%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(rabbit_prometheus_handler).

-export([init/2]).
-export([generate_response/2, content_types_provided/2, is_authorized/2]).
-export([setup/0]).

-include_lib("rabbitmq_web_dispatch/include/rabbitmq_web_dispatch_records.hrl").

-define(SCRAPE_DURATION, telemetry_scrape_duration_seconds).
-define(SCRAPE_SIZE, telemetry_scrape_size_bytes).

-define(AUTH_REALM, "Basic realm=\"RabbitMQ Prometheus\"").

%% ===================================================================
%% Cowboy Handler Callbacks
%% ===================================================================

init(Req, _State) ->
    {cowboy_rest, Req, #context{}}.


content_types_provided(ReqData, Context) ->
    %% Since Prometheus 2.0 Protobuf is no longer supported
    {[{{<<"text">>, <<"plain">>, '*'}, generate_response}], ReqData, Context}.

is_authorized(ReqData, Context) ->
    AuthSettings = rabbit_misc:get_env(rabbitmq_prometheus, authentication, []),
    case proplists:get_value(enabled, AuthSettings) of
        true ->
            rabbit_web_dispatch_access_control:is_authorized_monitor(ReqData,
                                                                     Context,
                                                                     #auth_settings{basic_auth_enabled = true,
                                                                                    auth_realm = ?AUTH_REALM});
        _ ->
            {true, ReqData, Context}
    end.


setup() ->
    setup_metrics(telemetry_registry()),
    setup_metrics('per-object'),
    setup_metrics('memory-breakdown'),
    setup_metrics('detailed').

setup_metrics(Registry) ->
    ScrapeDuration = [{name, ?SCRAPE_DURATION},
                      {help, "Scrape duration"},
                      {labels, ["registry", "content_type"]},
                      {registry, Registry}],
    ScrapeSize = [{name, ?SCRAPE_SIZE},
                  {help, "Scrape size, not encoded"},
                  {labels, ["registry", "content_type"]},
                  {registry, Registry}],

    prometheus_summary:declare(ScrapeDuration),
    prometheus_summary:declare(ScrapeSize).

%% ===================================================================
%% Private functions
%% ===================================================================

generate_response(ReqData, Context) ->
    Method = cowboy_req:method(ReqData),
    Response = gen_response(Method, ReqData),
    {stop, Response, Context}.

gen_response(<<"GET">>, Request) ->
    Registry0 = cowboy_req:binding(registry, Request, <<"default">>),
    case prometheus_registry:exists(Registry0) of
        false ->
          cowboy_req:reply(404, #{}, <<"Unknown Registry">>, Request);
        Registry ->
            put_filtering_options_into_process_dictionary(Request),
            gen_metrics_response(Registry, Request)
    end;
gen_response(_, Request) ->
    Request.

gen_metrics_response(Registry, Request) ->
    case validate_registry(Registry, registry()) of
        {true, RealRegistry} ->
            format_metrics(Request, RealRegistry);
        {registry_conflict, _ReqR, _ConfR} ->
            cowboy_req:reply(409, #{}, <<>>, Request)
    end.

format_metrics(Request, Registry) ->
    %% Formatting registries produces large binaries. Fullsweep eagerly to
    %% evict the large binaries faster and make GC cheaper.
    process_flag(fullsweep_after, 0),
    ContentType = prometheus_text_format:content_type(),
    Req = cowboy_req:stream_reply(200, #{<<"content-type">> => ContentType}, Request),
    Fmt = fun(Size, Data) ->
        cowboy_req:stream_body(Data, nofin, Req),
        Size + byte_size(Data)
    end,
    Size = prometheus_summary:observe_duration(
             Registry,
             ?SCRAPE_DURATION,
             [Registry, ContentType],
             fun () -> prometheus_text_format:format_into(Registry, Fmt, 0) end),
    cowboy_req:stream_body(<<>>, fin, Req),
    prometheus_summary:observe(Registry,
                               ?SCRAPE_SIZE,
                               [Registry, ContentType],
                               Size),
    Req.

validate_registry(undefined, auto) ->
    {true, default};
validate_registry(Registry, auto) ->
    {true, Registry};
validate_registry(Registry, Registry) ->
    {true, Registry};
validate_registry(Asked, Conf) ->
    {registry_conflict, Asked, Conf}.

telemetry_registry() ->
    application:get_env(rabbitmq_prometheus, telemetry_registry, default).

registry() ->
    application:get_env(rabbitmq_prometheus, registry, auto).

%% It's not easy to pass this information in a pure way (it'll require changing prometheus.erl)
put_filtering_options_into_process_dictionary(Request) ->
    #{vhost := VHosts, family := Families} = cowboy_req:match_qs([{vhost, [], undefined}, {family, [], undefined}], Request),
    case parse_vhosts(VHosts) of
        Vs when is_list(Vs) ->
            put(prometheus_vhost_filter, Vs);
        _ -> ok
    end,
    case parse_metric_families(Families) of
        Fs when is_list(Fs) ->
            put(prometheus_mf_filter, Fs);
        _ -> ok
    end,
    ok.

parse_vhosts(N) when is_binary(N) ->
    parse_vhosts([N]);
parse_vhosts(L) when is_list(L) ->
    [ VHostName || VHostName <- L, rabbit_vhost:exists(VHostName)];
parse_vhosts(_) ->
    false.

parse_metric_families(N) when is_binary(N) ->
    parse_metric_families([N]);
parse_metric_families([]) ->
    [];
parse_metric_families([B|Bs]) ->
    %% binary_to_existing_atom() should be enough, as it's used for filtering things out.
    %% Getting a full list of supported metrics would be harder.
    %% NB But on the other hand, it's nice to have validation. Implement it?
    case catch erlang:binary_to_existing_atom(B) of
        A when is_atom(A) ->
            [A|parse_metric_families(Bs)];
        _ ->
            parse_metric_families(Bs)
    end;
parse_metric_families(_) ->
    false.
