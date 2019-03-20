## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at https://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.


defmodule InformationUnitTest do
  use ExUnit.Case, async: true

  alias RabbitMQ.CLI.InformationUnit, as: IU

  test "bytes, MB, GB, TB are known units" do
    Enum.each(["bytes", "mb", "MB", "gb", "GB", "tb", "TB"],
              fn x -> assert IU.known_unit?(x) end)
  end

  test "glip-glops, millibars, gold pressed latinum bars and looney and are not known units" do
    Enum.each(["glip-glops", "millibars", "gold pressed latinum bars", "looney"],
              fn x -> assert not IU.known_unit?(x) end)
  end

  test "conversion to bytes" do
    assert IU.convert(0, "bytes") == 0
    assert IU.convert(100, "bytes") == 100
    assert IU.convert(9988, "bytes") == 9988
  end

  test "conversion to MB" do
    assert IU.convert(1000000, "mb") == 1.0
    assert IU.convert(9500000, "mb") == 9.5
    assert IU.convert(97893000, "mb") == 97.893
    assert IU.convert(978930000, "mb") == 978.93
  end

  test "conversion to GB" do
    assert IU.convert(978930000, "gb") == 0.9789

    assert IU.convert(1000000000, "gb") == 1.0
    assert IU.convert(9500000000, "gb") == 9.5
    assert IU.convert(97893000000, "gb") == 97.893
    assert IU.convert(978930000000, "gb") == 978.93
  end
end
