-module(rabbit_mgmt_http_bootstrap_js_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(rabbit_ct_broker_helpers, [rpc/4, rpc/5]).
-import(rabbit_mgmt_test_util, [http_get/2]).

-compile([export_all, nowarn_export_all]).

all() ->
    [
        bootstrap_js_default_test,
        bootstrap_js_oauth_enabled_test,
        bootstrap_js_sessions_enabled_test
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    inets:start(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE},
        {rmq_nodes_count, 1}
    ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
        rabbit_ct_broker_helpers:setup_steps() ++
        rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config, rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(bootstrap_js_oauth_enabled_test, Config) -> 
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                    [rabbitmq_management, oauth_enabled, true]),
                                
    ct:log("init_per_testcase bootstrap_js_oauth_enabled_test", []),
    rabbit_ct_helpers:testcase_started(Config, bootstrap_js_oauth_enabled_test),
    Config;

init_per_testcase(Testcase, Config) ->
    %% Reset configurations to defaults
    rpc(Config, 0, application, set_env, [rabbitmq_management, oauth_enabled, false]),
    rpc(Config, 0, application, set_env, [rabbitmq_management, sessions_enabled, false]),
    ct:log("init_per_testcase ", []),
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config.

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

bootstrap_js_default_test(Config) ->
    {ok, {{_Version, 200, _Reason}, Headers, Body}} = rabbit_mgmt_test_util:req(
        Config, 0, get_static, "js/bootstrap.js", []),
        
    ContentType = proplists:get_value("content-type", Headers),
    ?assertEqual("application/javascript; charset=utf-8", ContentType),
    
    %% Verify headers correctly specify no-cache
    CacheControl = proplists:get_value("cache-control", Headers, ""),
    ?assertMatch({match, _}, re:run(list_to_binary(CacheControl), <<"no-cache">>)),
    ?assertMatch({match, _}, re:run(list_to_binary(CacheControl), <<"no-store">>)),
    
    Pragma = proplists:get_value("pragma", Headers),
    ?assertEqual("no-cache", Pragma),
    
    Expires = proplists:get_value("expires", Headers),
    ?assertEqual("0", Expires),
    
    BodyBin = iolist_to_binary(Body),
    ?assertEqual(nomatch, re:run(BodyBin, <<"import './oidc-oauth/bootstrap\\.js'">>)),
    ?assertEqual(nomatch, re:run(BodyBin, <<"import './session\\.js'">>)),
    ?assertMatch({match, _}, re:run(BodyBin, <<"window\\.oauth = \\{enabled: false\\}">>)),
    ?assertMatch({match, _}, re:run(BodyBin, <<"window\\.sessions_enabled = function\\(\\) \\{ return false; \\}">>)),
    passed.

bootstrap_js_oauth_enabled_test(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                    [rabbitmq_management, oauth_enabled, true]),
    
    {ok, {{_Version, 200, _Reason}, _Headers, Body}} = rabbit_mgmt_test_util:req(
        Config, 0, get_static, "js/bootstrap.js", []),
        
    BodyBin = iolist_to_binary(Body),
    ?assertMatch({match, _}, re:run(BodyBin, <<"import './oidc-oauth/bootstrap\\.js'">>)),
    ?assertMatch({match, _}, re:run(BodyBin, <<"window\\.oauth = oauth_initialize_if_required\\(\\)">>)),
    passed.

bootstrap_js_sessions_enabled_test(Config) ->
    rpc(Config, 0, application, set_env, [rabbitmq_management, sessions_enabled, true]),

    {ok, {{_Version, 200, _Reason}, _Headers, Body}} = rabbit_mgmt_test_util:req(
        Config, 0, get_static, "js/bootstrap.js", []),
        
    BodyBin = iolist_to_binary(Body),
    ?assertMatch({match, _}, re:run(BodyBin, <<"import './session\\.js'">>)),
    ?assertMatch({match, _}, re:run(BodyBin, <<"window\\.sessions_enabled = function\\(\\) \\{ return true; \\}">>)),
    passed.

