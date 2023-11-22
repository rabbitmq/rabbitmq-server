## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule InfoKeysTest do
  use ExUnit.Case

  import RabbitMQ.CLI.Ctl.InfoKeys

  test "prepare translates aliases" do
    assert prepare_info_keys(["apple"], apple: :banana) == [banana: :apple]
  end

  test "prepare works without aliases" do
    assert prepare_info_keys(["apple"], []) == [:apple]
    assert prepare_info_keys(["apple"]) == [:apple]
  end

  test "validate translates aliases" do
    assert validate_info_keys(["apple"], ["banana"], apple: :banana) ==
             {:ok, [banana: :apple]}
  end

  test "validate works without aliases" do
    assert validate_info_keys(["apple"], ["apple"], []) == {:ok, [:apple]}
    assert validate_info_keys(["apple"], ["apple"]) == {:ok, [:apple]}
  end

  test "with_valid translates aliases" do
    assert with_valid_info_keys(["apple"], ["banana"], [apple: :banana], fn v -> v end) ==
             [:banana]
  end

  test "with_valid works without aliases" do
    assert with_valid_info_keys(["apple"], ["apple"], [], fn v -> v end) == [:apple]
    assert with_valid_info_keys(["apple"], ["apple"], fn v -> v end) == [:apple]
  end

  test "broker_keys preserves order" do
    keys = ["a", "b", "c"]
    broker_keys = prepare_info_keys(keys) |> broker_keys()
    assert broker_keys == [:a, :b, :c]
  end

  test "info_keys preserves requested key names" do
    aliases = [apple: :banana]
    broker_response = [banana: "bonono", carrot: "corrot"]

    keysA = ["banana", "carrot"]
    keysB = ["apple", "carrot"]

    normalizedA = prepare_info_keys(keysA, aliases)
    normalizedB = prepare_info_keys(keysB, aliases)

    assert :proplists.get_keys(normalizedA) == :proplists.get_keys(normalizedB)

    returnA = info_for_keys(broker_response, normalizedA)
    returnB = info_for_keys(broker_response, normalizedB)

    assert broker_keys(returnA) == Enum.map(keysA, &String.to_atom/1)
    assert broker_keys(returnB) == Enum.map(keysB, &String.to_atom/1)
  end
end
