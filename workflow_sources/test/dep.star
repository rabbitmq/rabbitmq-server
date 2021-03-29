load("rabbitmq_cli.lib.yml", "rabbitmq_cli_job")
load("ct.lib.yml", "checks_job", "ct_suites_job", "collect_job")
load("tests.lib.yml", "tests_job")

def dep_jobs(dep, erlang_version=None):
  jobs = {}
  if not getattr(dep, "skip_tests", False):
    if dep.name == "rabbitmq_cli":
      jobs[dep.name] = rabbitmq_cli_job(dep, erlang_version=erlang_version)
    elif getattr(dep, "test_suites_in_parallel", False):
      jobs[dep.name + "-checks"] = checks_job(dep, erlang_version=erlang_version)
      for suite in dep.suites:
        jobs[dep.name + "-ct-" + suite.name] = ct_suites_job(dep, suite.name, erlang_version=erlang_version)
      end
      jobs[dep.name] = collect_job(dep, erlang_version=erlang_version)
    else:
      jobs[dep.name] = tests_job(dep, erlang_version=erlang_version)
    end
  end
  return jobs
end
