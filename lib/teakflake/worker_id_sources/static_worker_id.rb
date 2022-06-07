# frozen_string_literal: true

module Teakflake
  class StaticWorkerId
    attr_reader :datacenter_id

    def initialize(zookeper, datacenter_id, worker_id, worker_id_zk_path: '/teakflake-servers')
      @zookeper = zookeper
      @datacenter_id = datacenter_id
      @worker_id = worker_id
      @worker_id_zk_path = worker_id_zk_path
    end

    def assert(time)
      @worker_id
    end

  private

    def peers
      begin
        @zookeper.get(@worker_id_zk_path)
      rescue ZK::Exceptions::NoNode
        logger.info(:missing_worker_id_path, path: @worker_id_zk_path)
        @zookeeper.create(@worker_id_zk_path, '', mode: :persistent)
      end

      children = @zookeeper.children(@worker_id_zk_path)
      peers = children.each_with_object({}) do |child, hash|
        info = @zookeeper.get("#{@worker_id_zk_path}/#{child}")
        hash[child.to_i] = info
      end

      logger.info(:found_peers, peers: peers)

      peers
    end

    def register_worker_id
      @zookeper.mkdir_p(@worker_id_zk_path)
      tries = 0
      begin
        @zookeeper.create(
          "#{@worker_id_zk_path}/#{@worker_id}", "#{`hostname -f`}:#{port}",
          mode: :ephemeral
        )
      rescue ZK::Exceptions::NodeExists
        if tries < 2
          logger.notice(:fail_attempt_claim_worker_id, id: @worker_id, tries: tries)
          tries += 1
          sleep 1
          retry
        else
          logger.error(:fail_claim_worker_id, id: @worker_id)
          raise
        end
      end
      logger.info(:claimed_worker_id, id: @worker_id)
    end

    def sanity_check_peers
    end
  end
end
