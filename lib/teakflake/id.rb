# frozen_string_literal: true

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

    attr_reader :timestamp, :datacenter_id, :worker_id, :sequence

    def initialize(id)
      @timestamp = (id >> TIMESTAMP_SHIFT) + EPOCH
      @datacenter_id = id >> DATACENTER_ID_SHIFT & (-1 ^ (-1 << DATACENTER_ID_BITS))
      @worker_id = id >> WORKER_ID_SHIFT & (-1 ^ (-1 << WORKER_ID_BITS))
      @sequence = id & SEQUENCE_MASK
    end
  end
end
