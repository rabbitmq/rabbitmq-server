ECS_CLUSTER_NAME ?= peer-discovery-aws-on-ecs-test
ECS_SERVICE_NAME ?= rabbitmq
ECS_TASK_FAMILY ?= rabbitmq
RABBITMQ_DEFAULT_USER ?= test
RABBITMQ_DEFAULT_PASS ?= 9ec61ba8362
RABBITMQ_ERLANG_COOKIE ?= peer-discovery-aws-on-ecs-test-cookie

LOGDIR ?= $(CURDIR)/log

define newline


endef

define RABBITMQ_CONFIG
cluster_formation.peer_discovery_backend = aws
cluster_formation.aws.instance_tags.service = rabbitmq

endef

# https://kichik.com/2020/09/10/mounting-configuration-files-in-fargate/
define REGISTER_TASK_JSON
{
    "family": "$(ECS_TASK_FAMILY)",
    "networkMode": "host",
    "containerDefinitions": [
        {
            "name": "rabbitmq",
            "image": "rabbitmq:3.8-management",
            "cpu": 256,
            "memory": 512,
            "portMappings": [
                {
                    "containerPort": 5672,
                    "hostPort": 5672,
                    "protocol": "tcp"
                },
                {
                    "containerPort": 15672,
                    "hostPort": 15672,
                    "protocol": "tcp"
                }
            ],
            "essential": true,
            "environment": [
                {
                    "name": "RABBITMQ_DEFAULT_USER",
                    "value": "$(RABBITMQ_DEFAULT_USER)"
                },
                {
                    "name": "RABBITMQ_DEFAULT_PASS",
                    "value": "$(RABBITMQ_DEFAULT_PASS)"
                },
                {
                    "name": "RABBITMQ_ERLANG_COOKIE",
                    "value": "$(RABBITMQ_ERLANG_COOKIE)"
                }
            ],
            "mountPoints": [
                {
                    "sourceVolume": "rabbitmq-conf-vol",
                    "containerPath": "/etc/rabbitmq"
                }
            ],
            "dependsOn": [
                {
                    "containerName": "rabbitmq-config",
                    "condition": "COMPLETE"
                }
            ]
        },
        {
            "name": "rabbitmq-config",
            "image": "bash",
            "cpu": 256,
            "memory": 512,
            "essential": false,
            "command": [
                "-c",
                "echo \\"[rabbitmq_management, rabbitmq_peer_discovery_aws].\\" > /etc/rabbitmq/enabled_plugins && echo $$DATA | base64 -d - | tee /etc/rabbitmq/rabbitmq.conf"
            ],
            "environment": [
                {
                    "name": "DATA",
                    "value": "$(shell printf '$(subst $(newline),\n,$(RABBITMQ_CONFIG))' | base64)"
                }
            ],
            "mountPoints": [
                {
                    "sourceVolume": "rabbitmq-conf-vol",
                    "containerPath": "/etc/rabbitmq"
                }
            ]
        }
    ],
    "volumes": [
        {
            "name": "rabbitmq-conf-vol",
            "host": {
                "sourcePath": "/rabbitmq-conf-vol"
            }
        }
    ],
    "requiresCompatibilities": [
        "EC2"
    ]
}
endef

.PHONY: cluster up print-task-json validate-task-json destroy

$(LOGDIR):
	mkdir -p $@

$(LOGDIR)/cluster: $(LOGDIR)
	ecs-cli configure \
	  --cluster $(ECS_CLUSTER_NAME) \
	  --default-launch-type EC2 \
	  --config-name $(ECS_CLUSTER_NAME) \
	  --region eu-west-1
	ecs-cli configure profile \
	  --access-key $(AWS_ACCESS_KEY_ID) \
	  --secret-key $(AWS_SECRET_ACCESS_KEY) \
	  --profile-name $(ECS_CLUSTER_NAME)-profile
	ecs-cli up \
	  --instance-role ecs-peer-discovery-aws \
	  --size 3 \
	  --instance-type t2.medium \
	  --keypair id_rsa_terraform \
	  --port 15672 \
	  --cluster-config $(ECS_CLUSTER_NAME) \
	  --ecs-profile $(ECS_CLUSTER_NAME)-profile \
	  --tags service=rabbitmq

	aws ec2 describe-instances \
		--cli-input-json "$$(aws ecs describe-container-instances \
			--cli-input-json "$$(aws ecs list-container-instances --cluster $(ECS_CLUSTER_NAME) | jq '{cluster: "$(ECS_CLUSTER_NAME)", containerInstances: .containerInstanceArns}')" | jq '{InstanceIds: .containerInstances | map(.ec2InstanceId)}')" \
				> $(LOGDIR)/instances.json
	jq '.Reservations | map(.Instances) | flatten | map(.NetworkInterfaces) | flatten | map(.Groups) | flatten | map(.GroupId) | unique' \
		$(LOGDIR)/instances.json > $(LOGDIR)/sgs.json
	aws ec2 authorize-security-group-ingress \
	  --group-id "$$(jq -r '.[0]' $(LOGDIR)/sgs.json)" \
	  --protocol tcp \
	  --port 1-65535 \
	  --source-group "$$(jq -r '.[0]' $(LOGDIR)/sgs.json)"
	rm $(LOGDIR)/sgs.json $(LOGDIR)/instances.json

	touch $@

print-task-json:
	@printf '$(subst $(newline),\n,$(REGISTER_TASK_JSON))\n'

validate-task-json:
	$(MAKE) --no-print-directory print-task-json | jq

$(LOGDIR)/task: $(LOGDIR)
	aws ecs register-task-definition \
	  --cli-input-json '$(shell echo '$(subst $(newline),,$(REGISTER_TASK_JSON))' | jq -cr)' \
	  > $@.tmp
	mv $@.tmp $@

$(LOGDIR)/service: $(LOGDIR) $(LOGDIR)/task
	aws ecs create-service \
	  --cluster $(ECS_CLUSTER_NAME) \
	  --service-name $(ECS_SERVICE_NAME) \
	  --desired-count 3 \
	  --launch-type EC2 \
	  --task-definition $(ECS_TASK_FAMILY) \
	  --tags key=service,value=rabbitmq \
	  > $@.tmp
	mv $@.tmp $@

cluster: $(LOGDIR)/cluster

up: $(LOGDIR)/service

define check_host
	test 3 -eq $$(curl -su $(RABBITMQ_DEFAULT_USER):$(RABBITMQ_DEFAULT_PASS) http://$(1):15672/api/nodes | jq 'map(.name) | length') &&
endef

test:
	$(foreach host,$(shell aws ec2 describe-instances --cli-input-json "$$(aws ecs describe-container-instances --cli-input-json "$$(aws ecs list-container-instances --cluster $(ECS_CLUSTER_NAME) | jq '{cluster: "$(ECS_CLUSTER_NAME)", containerInstances: .containerInstanceArns}')" \
	  | jq '{InstanceIds: .containerInstances | map(.ec2InstanceId)}')" | jq '.Reservations | map(.Instances) | flatten | map(.PublicDnsName)' \
	  | jq -r 'join(" ")'), \
		$(call check_host,$(host))) echo Success

down:
ifneq (,$(wildcard $(LOGDIR)/service))
	aws ecs delete-service \
	  --cluster $(ECS_CLUSTER_NAME) \
	  --service $(ECS_SERVICE_NAME) \
	  --force \
	  && rm $(LOGDIR)/service
endif
	
ifneq (,$(wildcard $(LOGDIR)/task))
	aws ecs deregister-task-definition \
	  --task-definition $(ECS_TASK_FAMILY):1 \
	  && rm $(LOGDIR)/task
endif

destroy:
ifneq (,$(wildcard $(LOGDIR)/cluster))
	ecs-cli down \
	  --force \
	  --cluster-config $(ECS_CLUSTER_NAME) \
	  --ecs-profile $(ECS_CLUSTER_NAME)-profile \
	&& rm $(LOGDIR)/cluster
endif
