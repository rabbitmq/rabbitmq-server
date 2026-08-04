-module(rabbit_mgmt_http_init_js_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(rabbit_ct_broker_helpers, [rpc/4]).
-import(rabbit_mgmt_test_util, [http_get/2, http_put/4, http_delete/3]).

-compile([export_all, nowarn_export_all]).

all() ->
    [
        unauthorized_test,
        init_js_settings_test,
        init_js_monitor_nodes_test,
        init_js_no_monitor_no_nodes_test
    ].

init_per_suite(Config) ->
    inets:start(),
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE},
        {rmq_nodes_count, 1}
    ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
        rabbit_ct_broker_helpers:setup_steps() ++
        rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config, rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    http_put(Config, "/users/test_user", [{password, <<"test_user">>}, {tags, <<"management">>}], {group, '2xx'}),
    http_put(Config, "/users/test_monitor", [{password, <<"test_monitor">>}, {tags, <<"monitoring">>}], {group, '2xx'}),
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    http_delete(Config, "/users/test_user", {group, '2xx'}),
    http_delete(Config, "/users/test_monitor", {group, '2xx'}),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

unauthorized_test(Config) ->
    %% Call endpoint with an unauthorized user (no management tag)
    %% Guest without valid password or totally unknown user will return 401
    Headers = [rabbit_mgmt_test_util:auth_header("unknown_user", "bad_password")],
    {ok, {{_Version, 401, Reason}, _Headers, Body}} = rabbit_mgmt_test_util:req(Config, 0, get_static, "js/init.js", Headers),
    ?assertEqual("Unauthorized", Reason),
    
    Decoded = rabbit_json:decode(Body),
    ?assertEqual(<<"not_authorized">>, maps:get(<<"error">>, Decoded)),
    ?assertEqual(<<"Not_Authorized">>, maps:get(<<"reason">>, Decoded)),
    passed.

init_js_settings_test(Config) ->
    %% Call the raw endpoint as a management user
    Headers = [rabbit_mgmt_test_util:auth_header("test_user", "test_user")],
    {ok, {{_Version, 200, _Reason}, ResponseHeaders, Body}} = rabbit_mgmt_test_util:req(Config, 0, get_static, "js/init.js", Headers),
        
    %% Check that the content type is javascript
    ContentType = proplists:get_value("content-type", ResponseHeaders),
    ?assertEqual("application/javascript; charset=utf-8", ContentType),
    
    %% Verify headers correctly specify no-cache
    CacheControl = proplists:get_value("cache-control", ResponseHeaders),
    ?assertMatch({match, _}, re:run(list_to_binary(CacheControl), <<"no-cache">>)),
    ?assertMatch({match, _}, re:run(list_to_binary(CacheControl), <<"no-store">>)),
    
    Pragma = proplists:get_value("pragma", ResponseHeaders),
    ?assertEqual("no-cache", Pragma),
    
    Expires = proplists:get_value("expires", ResponseHeaders),
    ?assertEqual("0", Expires),
    
    %% Convert body to binary for easy searching
    BodyBin = iolist_to_binary(Body),
    
    %% Verify the javascript contains the expected window variables
    ?assertMatch({match, _}, re:run(BodyBin, <<"window\\.app_settings = {">>)),
    ?assertMatch({match, _}, re:run(BodyBin, <<"window\\.app_vhosts = \\\[">>)),
    
    %% Verify the settings include the nested groups
    ?assertMatch({match, _}, re:run(BodyBin, <<"\"product_info\":">>)),
    ?assertMatch({match, _}, re:run(BodyBin, <<"\"sessions\":">>)),
    ?assertMatch({match, _}, re:run(BodyBin, <<"\"definitions\":">>)),
    passed.

init_js_monitor_nodes_test(Config) ->
    %% Call the raw endpoint as a monitoring user
    Headers = [rabbit_mgmt_test_util:auth_header("test_monitor", "test_monitor")],
    {ok, {{_Version, 200, _Reason}, _Headers, Body}} = rabbit_mgmt_test_util:req(Config, 0, get_static, "js/init.js", Headers),
        
    BodyBin = iolist_to_binary(Body),
    
    %% Verify the javascript contains the app_nodes variable since user has monitoring tag
    ?assertMatch({match, _}, re:run(BodyBin, <<"window\\.app_nodes = \\\[">>)),
    passed.

init_js_no_monitor_no_nodes_test(Config) ->
    %% Call the raw endpoint as a normal management user
    Headers = [rabbit_mgmt_test_util:auth_header("test_user", "test_user")],
    {ok, {{_Version, 200, _Reason}, _Headers, Body}} = rabbit_mgmt_test_util:req(Config, 0, get_static, "js/init.js", Headers),
        
    BodyBin = iolist_to_binary(Body),
    
    %% Verify the javascript DOES NOT contain the app_nodes variable
    ?assertEqual(nomatch, re:run(BodyBin, <<"window\\.app_nodes = \\\[">>)),
    passed.
