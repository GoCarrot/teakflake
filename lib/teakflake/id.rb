# frozen_string_literal: true

# Copyright 2022 Teak.io, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Teakflake
  class Id
    EPOCH = 1288834974657

    SEQUENCE_BITS = 12
    WORKER_ID_BITS = 5
    DATACENTER_ID_BITS = 5

    MAX_WORKER_ID = 2 ** WORKER_ID_BITS
    MAX_DATACENTER_ID = 2 ** DATACENTER_ID_BITS
    MAX_SEQUENCE = 2 ** SEQUENCE_BITS

    WORKER_ID_SHIFT = SEQUENCE_BITS
    DATACENTER_ID_SHIFT = WORKER_ID_SHIFT + WORKER_ID_BITS
    TIMESTAMP_SHIFT = DATACENTER_ID_SHIFT + DATACENTER_ID_BITS
    SEQUENCE_MASK = -1 ^ (-1 << SEQUENCE_BITS)

    attr_reader :timestamp, :datacenter_id, :worker_id, :sequence, :id

    def self.from_parts(timestamp, datacenter_id, worker_id, sequence)
      new(
        timestamp << TIMESTAMP_SHIFT |
        datacenter_id << DATACENTER_ID_SHIFT |
        worker_id << WORKER_ID_SHIFT |
        sequence
      )
    end

    def initialize(id)
      @id = id
      @timestamp = (id >> TIMESTAMP_SHIFT) + EPOCH
      @datacenter_id = id >> DATACENTER_ID_SHIFT & (-1 ^ (-1 << DATACENTER_ID_BITS))
      @worker_id = id >> WORKER_ID_SHIFT & (-1 ^ (-1 << WORKER_ID_BITS))
      @sequence = id & SEQUENCE_MASK
    end
  end
end
