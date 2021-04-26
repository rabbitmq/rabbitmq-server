ECS_CLUSTER_NAME ?= peer-discovery-aws-on-ecs-test
ECS_SERVICE_NAME ?= rabbitmq
ECS_TASK_FAMILY ?= rabbitmq
RABBITMQ_IMAGE ?= rabbitmq:3.8-management
RABBITMQ_DEFAULT_USER ?= test
RABBITMQ_DEFAULT_PASS ?= 9ec61ba8362
RABBITMQ_ERLANG_COOKIE ?= peer-discovery-aws-on-ecs-test-cookie

LOGDIR ?= $(CURDIR)/log

define newline


endef

define RABBITMQ_TAGGED_CONFIG
cluster_formation.peer_discovery_backend = aws
cluster_formation.aws.instance_tags.service = rabbitmq

endef

define RABBITMQ_AUTOSCALING_CONFIG
cluster_formation.peer_discovery_backend = aws
cluster_formation.aws.use_autoscaling_group = true

endef

# https://kichik.com/2020/09/10/mounting-configuration-files-in-fargate/
define REGISTER_TASK_JSON
{
    "family": "$(1)",
    "networkMode": "host",
    "containerDefinitions": [
        {
            "name": "rabbitmq",
            "image": "$(RABBITMQ_IMAGE)",
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
                    "value": "$(shell printf '$(subst $(newline),\n,$(2))' | base64)"
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

$(LOGDIR)/cluster:
	mkdir -p $@
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
	touch $@

$(LOGDIR)/adjust-sg: $(LOGDIR)/cluster
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
	@printf '$(subst $(newline),\n,$(call REGISTER_TASK_JSON, $(ECS_TASK_FAMILY)-tagged, $(RABBITMQ_TAGGED_CONFIG)))\n'

validate-task-json:
	$(MAKE) --no-print-directory print-task-json | jq

$(LOGDIR)/task-tagged:
	aws ecs register-task-definition \
	  --cli-input-json '$(shell echo '$(subst $(newline),,$(call REGISTER_TASK_JSON,$(ECS_TASK_FAMILY)-tagged, $(RABBITMQ_TAGGED_CONFIG)))' | jq -cr)' \
	  > $@.tmp
	mv $@.tmp $@

$(LOGDIR)/task-autoscaled:
	aws ecs register-task-definition \
	  --cli-input-json '$(shell echo '$(subst $(newline),,$(call REGISTER_TASK_JSON,$(ECS_TASK_FAMILY)-autoscaled, $(RABBITMQ_AUTOSCALING_CONFIG)))' | jq -cr)' \
	  > $@.tmp
	mv $@.tmp $@

$(LOGDIR)/service-tagged: $(LOGDIR)/task-tagged $(LOGDIR)/adjust-sg
	aws ecs create-service \
	  --cluster $(ECS_CLUSTER_NAME) \
	  --service-name $(ECS_SERVICE_NAME)-tagged \
	  --desired-count 3 \
	  --launch-type EC2 \
	  --task-definition $(ECS_TASK_FAMILY)-tagged \
	  --tags key=service,value=rabbitmq \
	  > $@.tmp
	mv $@.tmp $@

$(LOGDIR)/service-autoscaled: $(LOGDIR)/task-autoscaled $(LOGDIR)/adjust-sg
	aws ecs create-service \
	  --cluster $(ECS_CLUSTER_NAME) \
	  --service-name $(ECS_SERVICE_NAME)-autoscaled \
	  --desired-count 3 \
	  --launch-type EC2 \
	  --task-definition $(ECS_TASK_FAMILY)-autoscaled \
	  > $@.tmp
	mv $@.tmp $@

cluster: $(LOGDIR)/cluster

up: $(LOGDIR)/service-tagged $(LOGDIR)/service-autoscaled

define check_host
	test 3 -eq $$(curl -su $(RABBITMQ_DEFAULT_USER):$(RABBITMQ_DEFAULT_PASS) http://$(1):15672/api/nodes | jq 'map(.name) | length') &&
endef

test-%:
	curl -su $(RABBITMQ_DEFAULT_USER):$(RABBITMQ_DEFAULT_PASS) http://$(subst test-,,$@):15672/api/nodes | jq 'map(.name)' | tee $@.json
	test 3 -eq $$(jq 'length' $@.json)

test:
	$(foreach host,$(shell aws ec2 describe-instances --cli-input-json "$$(aws ecs describe-container-instances --cli-input-json "$$(aws ecs list-container-instances --cluster $(ECS_CLUSTER_NAME) | jq '{cluster: "$(ECS_CLUSTER_NAME)", containerInstances: .containerInstanceArns}')" \
	  | jq '{InstanceIds: .containerInstances | map(.ec2InstanceId)}')" | jq '.Reservations | map(.Instances) | flatten | map(.PublicDnsName)' \
	  | jq -r 'join(" ")'), \
		$(MAKE) test-$(host) &&) echo Success

down:
ifneq (,$(wildcard $(LOGDIR)/service-tagged))
	aws ecs delete-service \
	  --cluster $(ECS_CLUSTER_NAME) \
	  --service $(ECS_SERVICE_NAME)-tagged \
	  --force \
	  && rm $(LOGDIR)/service-tagged
endif

ifneq (,$(wildcard $(LOGDIR)/service-autoscaled))
	aws ecs delete-service \
	  --cluster $(ECS_CLUSTER_NAME) \
	  --service $(ECS_SERVICE_NAME)-autoscaled \
	  --force \
	  && rm $(LOGDIR)/service-autoscaled
endif

ifneq (,$(wildcard $(LOGDIR)/task-tagged))
	aws ecs deregister-task-definition \
	  --task-definition $(ECS_TASK_FAMILY)-tagged:1 \
	  && rm $(LOGDIR)/task-tagged
endif

ifneq (,$(wildcard $(LOGDIR)/task-autoscaled))
	aws ecs deregister-task-definition \
	  --task-definition $(ECS_TASK_FAMILY)-autoscaled:1 \
	  && rm $(LOGDIR)/task-autoscaled
endif

destroy:
ifneq (,$(wildcard $(LOGDIR)/cluster))
	ecs-cli down \
	  --force \
	  --cluster-config $(ECS_CLUSTER_NAME) \
	  --ecs-profile $(ECS_CLUSTER_NAME)-profile \
	&& rm $(LOGDIR)/cluster
endif
