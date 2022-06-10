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

require 'logsformyfamily'

module Teakflake
  class StaticWorkerId
    include LogsForMyFamily::LocalLogger

    attr_reader :datacenter_id, :clock

    def initialize(zookeeper, datacenter_id, worker_id, addr, clock, worker_id_zk_path: '/teakflake-servers')
      @zookeeper = zookeeper
      @datacenter_id = datacenter_id
      @worker_id = worker_id
      @worker_id_zk_path = worker_id_zk_path
      @addr = addr
      @clock = clock
      @id_registered = false
    end

    def register_worker_id
      logger.info(:claiming_worker_id, id: @worker_id)
      @zookeeper.mkdir_p(@worker_id_zk_path)
      tries = 0
      begin
        @zookeeper.create(
          "#{@worker_id_zk_path}/#{@worker_id}", @addr,
          mode: :ephemeral
        )
      rescue ZK::Exceptions::NodeExists
        if tries < 2
          logger.notice(:fail_attempt_claim_worker_id, id: @worker_id, tries: tries)
          tries += 1
          ::Kernel.sleep 1
          retry
        else
          logger.error(:fail_claim_worker_id, id: @worker_id)
          raise
        end
      end
      @id_registered = true
      logger.info(:claimed_worker_id, id: @worker_id)
    end

    def assert(_time)
      raise 'worker_id not registered' unless @id_registered
      @worker_id
    end

    def sanity_check_peers
      timestamps = peers.each_with_object([]) do |(worker_id, uri), timestamps|
        next if uri == @addr
        uri = URI(uri)
        uri.path = '/id'

        id = Teakflake::Id.new(get_id(uri))

        if id.worker_id != worker_id
          logger.error(:worker_id_insanity, expected: worker_id, got: id.worker_id, peer: uri)
          raise 'worker id insanity'
        end

        if id.datacenter_id != @datacenter_id
          logger.error(:datacenter_id_insanity, expected: @datacenter_id, got: id.datacenter_id, peer: uri)
          raise 'datacenter id insanity'
        end
        timestamps << id.timestamp
      end

      if !timestamps.empty?
        avg = timestamps.inject(:+) / timestamps.length.to_f
        our_time = @clock.millis
        if (our_time - avg).abs > 10_000
          logger.error(:timestamp_insanity, avg: avg, our_time: our_time)
          raise 'timestamp insanity'
        end
      end
    end

  private

    def get_id(uri)
      request = Net::HTTP::Post.new(uri.request_uri, { 'Accept' => 'application/json' })
      response = Net::HTTP.start(uri.host, uri.port) do |http|
        http.request(request)
      end
      JSON.parse(response.body).dig('response', 'ids', 0)
    end

    def peers
      begin
        @zookeeper.get(@worker_id_zk_path)
      rescue ZK::Exceptions::NoNode
        logger.info(:missing_worker_id_path, path: @worker_id_zk_path)
        @zookeeper.create(@worker_id_zk_path, '', mode: :persistent)
      end

      children = @zookeeper.children(@worker_id_zk_path)
      peers = children.each_with_object({}) do |child, hash|
        info = @zookeeper.get("#{@worker_id_zk_path}/#{child}")
        hash[child.to_i] = info[0]
      end

      logger.info(:found_peers, peers: peers)

      peers
    end
  end
end
