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
  class DynamicWorkerId
    RESERVED_MILLIS = 2_000

    def initialize(zookeper, datacenter_id)
      @zookeper = zookeper
      @datacenter_id = datacenter_id
      @worker_id = acquire_worker_id
    end

    def assert(time)

    end

  private

    def assert_worker_id_ownership
    end

    def acquire_prospective_worker_id
      acquire_path = "/teakflake/worker-id-gen/#{@datacenter_id}"
      node = @zookeper.create("#{acquire_path}/acquire-", mode: :ephemeral_sequential)
      stat = @zookeper.stat(node)
      acquire_time = stat.ctime
      @zookeeper.delete(node)
      [node.split('-')[1].to_i % Id::MAX_WORKER_ID, acquire_time]
    rescue ZK::Exceptions::NoNode
      @zookeper.mkdir_p(acquire_path)
      retry
    end

    def try_steal_ownership(node, acquire_time)
      orig_data, stat = @zookeper.get(node)
      end_reserved_time = stat.mtime + RESERVED_MILLIS
      if end_reserved_time < acquire_time
        stat = @zookeper.set(node, our_info, version: stat.version)
        if stat.mtime < end_reserved_time
          @zookeper.set(node, orig_data, version: stat.version, ignore: :bad_version)
          return nil
        else
          return stat
        end
      else
        nil
      end
    rescue ZK::Exceptions::BadVersion
      return nil
    end

    def try_acquire_worker_id
      id, acquire_time = acquire_prospective_worker_id
      acquire_path = "/teakflake/workers/#{@datacenter_id}"
      our_path = "#{acquire_path}/#{id}"
      begin
        node = @zookeeper.create(our_path, ourinfo, mode: :persistent, ignore: :node_exists)
        stat =
          if node
            data = @zookeper.get(node)
            if data[0] == ourinfo
              @zookeper.set(node, ourinfo, version: data[1].version, ignore: :bad_version)
            else
              nil
            end
          else
            try_steal_ownership(our_path, acquire_time)
          end
        return nil if !stat

        @asserted_through = stat.mtime + RESERVED_MILLIS
        @last_asserted_version = stat.version
        return id
      rescue ZK::Exceptions::NoNode
        @zookeper.mkdir_p(acquire_path)
        retry
      rescue ZK::Exceptions::BadVersion
        return nil
      end
    end

    def acquire_worker_id
      attempts = 0
      loop do
        attempts += 1
        worker_id = try_acquire_worker_id
        if !worker_id
          raise "Could not get a worker id" if attempts > MAX_ATTEMPTS
          next
        end
        break worker_id
      end
    end
  end
end
