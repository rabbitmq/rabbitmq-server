#!/usr/bin/env bash

gem list cf-uaac | grep cf-uaac || exit 1
gem list bunny | grep bunny || exit 1

target=${UAA_HOST:="http://localhost:8080/uaa"}
# export to use a different ctl, e.g. if the node was built from source
ctl=${RABBITMQCTL:="rabbitmqctl"}

# Target the server
uaac target $target

# Get admin client to manage UAA
uaac token client get admin -s adminsecret

# Set permission to list signing keys
uaac client update admin --authorities "clients.read clients.secret clients.write uaa.admin clients.admin scim.write scim.read uaa.resource"

# Rabbit UAA user
# The user name and password needed to retrieve access tokens,
# RabbiMQ does not have to be aware of this credentials.
uaac user add rabbit_super -p rabbit_super --email rabbit_super@example.com
uaac user add rabbit_nosuper -p rabbit_nosuper --email rabbit_nosuper@example.com

# Create permissions
# Uaa groups will become scopes, which should define RabbitMQ permissions and tags.
uaac group add "rabbitmq.read:*/*"
uaac group add "rabbitmq.write:*/*"
uaac group add "rabbitmq.configure:*/*"

uaac group add "rabbitmq.write:uaa_vhost/*"
uaac group add "rabbitmq.read:uaa_vhost/some*"

uaac group add "rabbitmq.tag:administrator"

# Assigning groups to users.
# rabbit_super will be able to read,write and configure any resources.
# They will be able to connect to the management plugin as well.
uaac member add "rabbitmq.read:*/*" rabbit_super
uaac member add "rabbitmq.write:*/*" rabbit_super
uaac member add "rabbitmq.configure:*/*" rabbit_super
uaac member add "rabbitmq.tag:administrator" rabbit_super

# rabbit_nosuper will be able to read uaa_vhost resources starting with some
# and write to any uaa_vhost resources
uaac member add "rabbitmq.write:uaa_vhost/*" rabbit_nosuper
uaac member add "rabbitmq.read:uaa_vhost/some*" rabbit_nosuper

# Configure RabbiqMQ
# Add uaa_vhost and other vhost to check permissions
$ctl add_vhost uaa_vhost
$ctl add_vhost other_vhost

# add e.g. --access_token_validity 60 --refresh_token_validity 3600 to experiment with token validity
uaac client add rabbit_client --name rabbit_client --scope 'rabbitmq.*' --authorized_grant_types password,client_credentials --authorities rabbitmq --secret rabbit_secret --redirect_uri 'http://localhost:15672'

# Set guest user permissions to create queues.
$ctl set_permissions -p uaa_vhost guest '.*' '.*' '.*'
$ctl set_permissions -p other_vhost guest '.*' '.*' '.*'

# Get access tokens
uaac token owner get rabbit_client rabbit_super -s rabbit_secret -p rabbit_super
uaac token owner get rabbit_client rabbit_nosuper -s rabbit_secret -p rabbit_nosuper

echo "Auth info for rabbit_nosuper user
This user will have read access to uaa_vhost resources,
which name start with some and write access to all uaa_vhost resources.
Use access_token as a RabbitMQ password, the username is ignored"
echo
uaac context rabbit_nosuper
echo

echo "Auth info for rabbit_super user
This user will have full access to all rabbitmq vhosts.
Use access_token as a RabbitMQ password, the username is ignored"
echo
uaac context rabbit_super
echo

# Create queues
ruby demo/declare_queues.rb uaa_vhost/some_queue uaa_vhost/other_queue other_vhost/some_queue other_vhost/other_queue
