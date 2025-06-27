-record(rabbit_cli, {progname :: string(),
                     args :: argparse:args(),
                     argparse_def :: argparse:command() | undefined,
                     arg_map :: argparse:arg_map() | undefined,
                     cmd_path :: argparse:cmd_path() | undefined,
                     command :: argparse:command() | undefined,
                     legacy :: boolean() | undefined,

                     os :: {unix | win32, atom()},
                     client :: #{hostname := string(),
                                 proto := atom()} | undefined,
                     env :: [{os:env_var_name(), os:env_var_value()}],
                     terminal :: #{stdout := boolean(),
                                   stderr := boolean(),
                                   stdin := boolean(),

                                   name := eterminfo:term_name(),
                                   info := eterminfo:terminfo()},

                     priv}).
