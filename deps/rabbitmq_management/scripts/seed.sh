#!/usr/bin/env sh

# use admin context to add user and client
uaac token client get admin -s adminsecret

uaac user add rabbit_user -p rabbit_password --email rabbit_user@example.com

# these groups will end up in the scope of the users
uaac group add "rabbitmq.read:*/*"
uaac group add "rabbitmq.write:*/*"
uaac group add "rabbitmq.configure:*/*"
uaac group add "rabbitmq.tag:management"
uaac group add "rabbitmq.tag:administrator"

uaac member add "rabbitmq.read:*/*" rabbit_user
uaac member add "rabbitmq.write:*/*" rabbit_user
uaac member add "rabbitmq.configure:*/*" rabbit_user
uaac member add "rabbitmq.tag:management" rabbit_user
uaac member add "rabbitmq.tag:administrator" rabbit_user

# add the client for the management plugin. It has the implicit grant type.
# add e.g. --access_token_validity 60 --refresh_token_validity 3600 to experiment with token validity
uaac client add rabbit_user_client \
 --name rabbit_user_client \
 --secret '' \
 --scope 'rabbitmq.* openid' \
 --authorized_grant_types implicit \
 --autoapprove true \
 --redirect_uri 'http://localhost:15672/**'
