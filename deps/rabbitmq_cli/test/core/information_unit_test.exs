## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule InformationUnitTest do
  use ExUnit.Case, async: true

  alias RabbitMQ.CLI.InformationUnit, as: IU

  test "bytes, MB, GB, TB are known units" do
    Enum.each(
      ["bytes", "mb", "MB", "gb", "GB", "tb", "TB"],
      fn x -> assert IU.known_unit?(x) end
    )
  end

  test "glip-glops, millibars, gold pressed latinum bars and looney and are not known units" do
    Enum.each(
      ["glip-glops", "millibars", "gold pressed latinum bars", "looney"],
      fn x -> assert not IU.known_unit?(x) end
    )
  end

  test "conversion to bytes" do
    assert IU.convert(0, "bytes") == 0
    assert IU.convert(100, "bytes") == 100
    assert IU.convert(9988, "bytes") == 9988
  end

  test "conversion to MB" do
    assert IU.convert(1_000_000, "mb") == 1.0
    assert IU.convert(9_500_000, "mb") == 9.5
    assert IU.convert(97_893_000, "mb") == 97.893
    assert IU.convert(978_930_000, "mb") == 978.93
  end

  test "conversion to GB" do
    assert IU.convert(978_930_000, "gb") == 0.9789

    assert IU.convert(1_000_000_000, "gb") == 1.0
    assert IU.convert(9_500_000_000, "gb") == 9.5
    assert IU.convert(97_893_000_000, "gb") == 97.893
    assert IU.convert(978_930_000_000, "gb") == 978.93
  end
end
