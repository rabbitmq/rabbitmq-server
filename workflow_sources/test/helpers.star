load("@ytt:data", "data")

def ci_image_tag(erlang_version):
  return "erlang-" + erlang_version + "-rabbitmq-${{ github.sha }}"
end

def ci_image(erlang_version):
  return "eu.gcr.io/cf-rabbitmq-core/ci:" + ci_image_tag(erlang_version)
end

def ci_dep_image(erlang_version, dep_name):
  return "eu.gcr.io/cf-rabbitmq-core/ci-" + dep_name + ":" + ci_image_tag(erlang_version)
end

def skip_ci_condition():
  return "!contains(github.event.head_commit.message, '[ci skip]')"
end
