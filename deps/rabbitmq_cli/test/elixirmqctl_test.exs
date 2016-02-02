defmodule ElixirMQCtlTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureIO

  setup_all do
    :net_kernel.start([:elixirmqctl, :shortnames])
    on_exit([], fn -> :net_kernel.stop() end)
    :ok
  end

  setup context do
    :ok
  end

  test "status shows PID", context do
    assert capture_io(fn -> ElixirMQCtl.main(["status"]) end) =~ ~r/PID\: \d+/
  end

  test "status shows running apps", context do
    assert capture_io(fn -> ElixirMQCtl.main(["status"]) end) =~ ~r/Applications currently running\:\n/
    assert capture_io(fn -> ElixirMQCtl.main(["status"]) end) =~ ~r/---------------------------------------\n/
    assert capture_io(fn -> ElixirMQCtl.main(["status"]) end) =~ ~r/\[rabbit\]\s*| RabbitMQ\s*| \d+.\d+.\d+\n/
  end

  test "print error message on a bad connection", context do
    command = ["status", "-n", "sandwich@pastrami"]
    assert capture_io(fn -> ElixirMQCtl.main(command) end) =~ ~r/unable to connect to node 'sandwich@pastrami'\: nodedown/
  end

  test "Empty command shows usage message" do
    assert capture_io(fn -> ElixirMQCtl.main([]) end) =~ ~r/Usage\:/
  end

  test "Empty command with options shows usage message" do
    assert capture_io(fn -> ElixirMQCtl.main(["-n", "sandwich@pastrami"]) end) =~ ~r/Usage\:/
  end
end
