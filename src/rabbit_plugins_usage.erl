%% Generated, do not edit!
-module(rabbit_plugins_usage).
-export([usage/0]).
usage() -> "Usage:
rabbitmq-plugins [-n <node>] <command> [<command options>] 

Commands:
    list [-v] [-m] [-E] [-e] [<pattern>]
    enable [--offline] [--online] <plugin> ...
    disable [--offline] [--online] <plugin> ...
    set [--offline] [--online] <plugin> ...


".
