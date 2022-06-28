-module(cth_log_redirect_any_domains).

-export([log/2]).

-define(BACKEND_MODULE, cth_log_redirect).

%% Reversed behavior compared to `cth_log_redirect': log events with an
%% unknown domain are sent to the `cth_log_redirect' server, others are
%% dropped (as they are already handled by `cth_log_redirect').
log(#{msg:={report,_Msg},meta:=#{domain:=[otp,sasl]}},_Config) ->
    ok;
log(#{meta:=#{domain:=[otp]}},_Config) ->
    ok;
log(#{meta:=#{domain:=_}}=Log,Config) ->
    do_log(add_log_category(Log,error_logger),Config);
log(_Log,_Config) ->
    ok.

add_log_category(#{meta:=Meta}=Log,Category) ->
    Log#{meta=>Meta#{?BACKEND_MODULE=>#{category=>Category}}}.

do_log(Log,Config) ->
    gen_server:call(?BACKEND_MODULE,{log,Log,Config}).
