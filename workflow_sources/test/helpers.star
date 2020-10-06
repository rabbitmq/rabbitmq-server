load("@ytt:data", "data")

def ci_image_tag():
  return "erlang-" + data.values.erlang_version + "-rabbitmq-${{ github.sha }}"
end

def ci_image():
  return "eu.gcr.io/cf-rabbitmq-core/ci:" + ci_image_tag()
end

def ci_dep_image(dep_name):
  return "eu.gcr.io/cf-rabbitmq-core/ci-" + dep_name + ":" + ci_image_tag()
end
