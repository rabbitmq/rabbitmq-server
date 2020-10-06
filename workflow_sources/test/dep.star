load("util.star", "group_by_time")
load("rabbitmq_cli.lib.yml", "rabbitmq_cli_job")
load("ct.lib.yml", "checks_job", "ct_suites_job", "collect_job")
load("tests.lib.yml", "tests_job")

def dep_jobs(dep):
  jobs = {}
  if not getattr(dep, "skip_tests", False):
    if dep.name == "rabbitmq_cli":
      jobs[dep.name] = rabbitmq_cli_job(dep)
    elif len(dep.suites) > 20:
      jobs[dep.name + "-checks"] = checks_job(dep)
      for group in group_by_time(dep.suites):
        jobs[dep.name + "-ct-" + group["name"]] = ct_suites_job(dep, group)
      end
      jobs[dep.name] = collect_job(dep)
    else:
      jobs[dep.name] = tests_job(dep)
    end
  end
  return jobs
end
