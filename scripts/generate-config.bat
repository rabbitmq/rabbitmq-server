@echo off
del ".\generated\rabbitmq.config" && del ".\generated\rabbitmq.*.config" && "%ERLANG_HOME%\bin\escript.exe" .\cuttlefish -i .\rabbitmq.schema %* -f rabbitmq && ren ".\generated\rabbitmq.*.config" "rabbitmq.config"
