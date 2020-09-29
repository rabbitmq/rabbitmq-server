load("@ytt:data", "data")

def should_skip_dialyze(dep):
  return (data.values.versions.erlang != "23.0"
          or (hasattr(dep, "skip_dialyzer") and dep.skip_dialyzer))
end

def should_skip_xref(dep):
  return hasattr(dep, "skip_xref") and dep.skip_xref
end

def ci_image_tag():
  return "erlang-" + data.values.versions.erlang + "-rabbitmq-${{ github.sha }}"
end

def ci_image():
  return "eu.gcr.io/cf-rabbitmq-core/ci:" + ci_image_tag()
end

def ci_dep_image(dep_name):
  return "eu.gcr.io/cf-rabbitmq-core/ci-" + dep_name + ":" + ci_image_tag()
end
