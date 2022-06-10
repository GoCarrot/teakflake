# frozen_string_literal: true

require_relative 'id'

module Teakflake
  class IdWorker

    class BackwardsTimeError < RuntimeError
    end

    def initialize(worker_id_assigner)
      @clock = worker_id_assigner.clock
      @sequence = 0
      @worker_id_assigner = worker_id_assigner
      @datacenter_id = worker_id_assigner.datacenter_id
      @last_time = 0
    end

    def id(requested_count = 1)
      if requested_count < 1
        raise 'Must request at least one id'
      end

      time = @clock.millis
      if time < @last_time
        raise BackwardsTimeError, "Clocked moved backwards. Refusing to generate id for #{@last_time - time} milliseconds"
      end

      worker_id = @worker_id_assigner.assert(time)

      @sequence = 0 if time > @last_time

      if Id::MAX_SEQUENCE == @sequence
        time = til_next_millis
        @sequence = 0
      end

      available_count = Id::MAX_SEQUENCE - @sequence
      requested_count =
        if available_count < requested_count
          available_count
        else
          requested_count
        end

      next_sequence = @sequence + requested_count

      @last_time = time
      timestamp_part = time - Id::EPOCH

      response = Array.new(requested_count) do |i|
        timestamp_part << Id::TIMESTAMP_SHIFT |
          @datacenter_id << Id::DATACENTER_ID_SHIFT |
          worker_id << Id::WORKER_ID_SHIFT |
          @sequence + i
      end

      @sequence = next_sequence
      response
    end

    def til_next_millis
      loop do
        time = @clock.millis
        if time < @last_time
          raise BackwardsTimeError, "Clocked moved backwards. Refusing to generate id for #{@last_time - time} milliseconds"
        elsif time > @last_time
          break time
        end
      end
    end
  end
end
