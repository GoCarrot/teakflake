# frozen_string_literal: true

require 'teakflake/worker_id_sources/static_worker_id'
require 'teakflake/clocks/process'
require 'teakflake/id'

require 'zk'

RSpec.describe Teakflake::StaticWorkerId do
  let(:datacenter_id) { rand(Teakflake::Id::MAX_DATACENTER_ID) }
  let(:worker_id) { rand(Teakflake::Id::MAX_WORKER_ID) }
  let(:addr) { 'localhost:80' }
  let(:zookeeper) { instance_double(ZK::Client::Threaded) }
  let(:clock) { instance_double(Teakflake::ProcessClock) }

  subject(:id_assigner) { described_class.new(zookeeper, datacenter_id, worker_id, addr, clock) }

  describe '#register_worker_id' do
    before do
      allow(zookeeper).to receive(:mkdir_p).with('/teakflake-servers')
      allow(zookeeper).to receive(:create)
    end

    it 'creates the worker id path' do
      id_assigner.register_worker_id
      expect(zookeeper).to have_received(:mkdir_p).with('/teakflake-servers')
    end

    it 'creates the worker id node' do
      id_assigner.register_worker_id
      expect(zookeeper).to have_received(:create).with(
        "/teakflake-servers/#{worker_id}", addr, mode: :ephemeral
      )
    end

    it 'logs that it is claiming the worker id' do
      id_assigner.register_worker_id
      expect(@log_messages).to include(
        contain_exactly(:info, :claiming_worker_id, hash_including(id: worker_id)),
        contain_exactly(:info, :claimed_worker_id, hash_including(id: worker_id))
      )
    end

    context 'if the node temporarily exists' do
      before do
        allow(::Kernel).to receive(:sleep)
        allow(zookeeper).to receive(:create).and_invoke(
          proc { raise ZK::Exceptions::NodeExists },
          proc { raise ZK::Exceptions::NodeExists },
          proc { true }
        )
      end

      it 'creates the node' do
        expect { id_assigner.register_worker_id }.not_to raise_error
      end

      it 'logs the attempts' do
        id_assigner.register_worker_id
        expect(@log_messages).to include(
          contain_exactly(:notice, :fail_attempt_claim_worker_id, hash_including(id: worker_id, tries: 0)),
          contain_exactly(:notice, :fail_attempt_claim_worker_id, hash_including(id: worker_id, tries: 1)),
          contain_exactly(:info, :claimed_worker_id, hash_including(id: worker_id))
        )
      end

      it 'sleeps between attempts' do
        id_assigner.register_worker_id
        expect(::Kernel).to have_received(:sleep).with(1).exactly(2).times
      end
    end

    context 'if the node permanently exists' do
      before do
        allow(::Kernel).to receive(:sleep)
        allow(zookeeper).to receive(:create).and_raise(ZK::Exceptions::NodeExists)
      end

      it 'raises an error' do
        expect { id_assigner.register_worker_id }.to raise_error(ZK::Exceptions::NodeExists)
      end

      it 'logs the failure' do
        id_assigner.register_worker_id rescue nil
        expect(@log_messages).to include(
          contain_exactly(:notice, :fail_attempt_claim_worker_id, hash_including(id: worker_id, tries: 0)),
          contain_exactly(:notice, :fail_attempt_claim_worker_id, hash_including(id: worker_id, tries: 1)),
          contain_exactly(:error, :fail_claim_worker_id, hash_including(id: worker_id))
        )
      end
    end
  end

  describe '#datacenter_id' do
    it 'returns the datacenter id' do
      expect(id_assigner.datacenter_id).to eq(datacenter_id)
    end
  end

  describe '#assert' do
    context 'if the worker id is not registered' do
      it 'raises an error' do
        expect { id_assigner.assert(nil) }.to raise_error('worker_id not registered')
      end
    end

    context 'when the worker id is registered' do
      before do
        allow(zookeeper).to receive(:mkdir_p).with('/teakflake-servers')
        allow(zookeeper).to receive(:create)
        id_assigner.register_worker_id
      end

      it 'returns the worker id' do
        expect(id_assigner.assert(nil)).to eq worker_id
      end
    end
  end
end
