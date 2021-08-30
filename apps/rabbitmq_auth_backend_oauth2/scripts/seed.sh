#!/usr/bin/env sh

uaac token client get admin -s adminsecret

uaac client delete rabbit_client
uaac client add rabbit_client --name rabbit_client \
 --secret rabbit_secret \
 --authorized_grant_types client_credentials \
 --authorities 'rabbitmq.read:*/* rabbitmq.write:*/* rabbitmq.configure:*/* rabbitmq.tag:management rabbitmq.tag:administrator' \
 --access_token_validity 86400

uaac token client get rabbit_client -s rabbit_secret

uaac token client get admin -s adminsecret

uaac context rabbit_client

# switch back to the admin context so that we have
# the permissions to add the user
uaac token client get admin -s adminsecret

uaac user add rabbit_user -p rabbit_password --email rabbit_user@example.com

uaac group add "rabbitmq.read:*/*"
uaac group add "rabbitmq.write:*/*"
uaac group add "rabbitmq.configure:*/*"

uaac member add "rabbitmq.read:*/*" rabbit_user
uaac member add "rabbitmq.write:*/*" rabbit_user
uaac member add "rabbitmq.configure:*/*" rabbit_user

uaac client add rabbit_user_client \
 --name rabbit_user_client \
 --scope 'rabbitmq.*' \
 --authorized_grant_types password \
 --secret rabbit_secret \
 --redirect_uri 'http://localhost:15672'

uaac token owner get rabbit_user_client rabbit_user -s rabbit_secret -p rabbit_password

uaac token client get admin -s adminsecret

uaac context rabbit_client
