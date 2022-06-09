# frozen_string_literal: true

require 'teakflake/worker_id_sources/static_worker_id'
require 'teakflake/clocks/process'
require 'teakflake/id'

require 'zk'

RSpec.describe Teakflake::StaticWorkerId do
  let(:datacenter_id) { rand(Teakflake::Id::MAX_DATACENTER_ID) }
  let(:worker_id) { rand(Teakflake::Id::MAX_WORKER_ID) }
  let(:addr) { 'http://localhost:80' }
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

  describe '#sanity_check_peers' do
    context 'with no peers' do
      before do
        allow(zookeeper).to receive(:get).with('/teakflake-servers')
        allow(zookeeper).to receive(:children).with('/teakflake-servers').and_return([])
      end

      it 'passes' do
        expect { id_assigner.sanity_check_peers }.not_to raise_error
      end
    end

    context 'with no worker id node' do
      before do
        allow(zookeeper).to receive(:get).with('/teakflake-servers').and_raise(ZK::Exceptions::NoNode)
        allow(zookeeper).to receive(:create)
        allow(zookeeper).to receive(:children).with('/teakflake-servers').and_return([])
      end

      it 'passes' do
        expect { id_assigner.sanity_check_peers }.not_to raise_error
      end

      it 'creates the node' do
        id_assigner.sanity_check_peers
        expect(zookeeper).to have_received(:create).with('/teakflake-servers', '', mode: :persistent)
      end

      it 'logs that it created the node' do
        id_assigner.sanity_check_peers
        expect(@log_messages).to include(
          contain_exactly(:info, :missing_worker_id_path, hash_including(path: '/teakflake-servers'))
        )
      end
    end

    context 'with sane peers' do
      let(:worker_id) { rand(Teakflake::Id::MAX_WORKER_ID - 2) + 2 }
      let(:peer0) { 'http://peer0:80' }
      let(:peer1) { 'http://peer1:80' }
      let(:id0) { Teakflake::Id.from_parts(50_000, datacenter_id, worker_id - 2, 0).id }
      let(:id1) { Teakflake::Id.from_parts(49_000, datacenter_id, worker_id - 1, 0).id }

      before do
        allow(zookeeper).to receive(:get).with('/teakflake-servers')
        allow(zookeeper).to receive(:children).with('/teakflake-servers').and_return([(worker_id - 2).to_s, (worker_id - 1).to_s])
        allow(zookeeper).to receive(:get).with("/teakflake-servers/#{worker_id - 2}").and_return(peer0)
        allow(zookeeper).to receive(:get).with("/teakflake-servers/#{worker_id - 1}").and_return(peer1)
        stub_request(:post, "#{peer0}/id").and_return(body: JSON.generate({ metadata: {}, response: { ids: [id0] } }))
        stub_request(:post, "#{peer1}/id").and_return(body: JSON.generate({ metadata: {}, response: { ids: [id1] } }))
        allow(clock).to receive(:millis).and_return(52_000 + Teakflake::Id::EPOCH)
      end

      it 'passes' do
        expect { id_assigner.sanity_check_peers }.not_to raise_error
      end

      it 'checks with peers' do
        id_assigner.sanity_check_peers
        expect(WebMock).to have_requested(:post, "#{peer0}/id")
        expect(WebMock).to have_requested(:post, "#{peer1}/id")
      end
    end

    context 'with worker id insanity' do
      let(:worker_id) { rand(Teakflake::Id::MAX_WORKER_ID - 2) + 2 }
      let(:peer0) { 'http://peer0:80' }
      let(:peer1) { 'http://peer1:80' }
      let(:id0) { Teakflake::Id.from_parts(50_000, datacenter_id, worker_id - 2, 0).id }
      let(:id1) { Teakflake::Id.from_parts(49_000, datacenter_id, worker_id - 1, 0).id }

      before do
        allow(zookeeper).to receive(:get).with('/teakflake-servers')
        allow(zookeeper).to receive(:children).with('/teakflake-servers').and_return([(worker_id - 2).to_s, (worker_id - 1).to_s])
        allow(zookeeper).to receive(:get).with("/teakflake-servers/#{worker_id - 2}").and_return(peer1)
        allow(zookeeper).to receive(:get).with("/teakflake-servers/#{worker_id - 1}").and_return(peer0)
        stub_request(:post, "#{peer0}/id").and_return(body: JSON.generate({ metadata: {}, response: { ids: [id0] } }))
        stub_request(:post, "#{peer1}/id").and_return(body: JSON.generate({ metadata: {}, response: { ids: [id1] } }))
        allow(clock).to receive(:millis).and_return(52_000 + Teakflake::Id::EPOCH)
      end

      it 'raises an error' do
        expect { id_assigner.sanity_check_peers }.to raise_error('worker id insanity')
      end

      it 'logs an error' do
        id_assigner.sanity_check_peers rescue nil
        expect(@log_messages).to include(
          contain_exactly(:error, :worker_id_insanity, hash_including(expected: worker_id - 2, got: worker_id - 1))
        )
      end
    end

    context 'with datacenter id instanity' do
      let(:worker_id) { rand(Teakflake::Id::MAX_WORKER_ID - 2) + 2 }
      let(:datacenter_id) { rand(Teakflake::Id::MAX_DATACENTER_ID - 2) + 2 }
      let(:peer0) { 'http://peer0:80' }
      let(:peer1) { 'http://peer1:80' }
      let(:id0) { Teakflake::Id.from_parts(50_000, datacenter_id - 1, worker_id - 2, 0).id }
      let(:id1) { Teakflake::Id.from_parts(49_000, datacenter_id, worker_id - 1, 0).id }

      before do
        allow(zookeeper).to receive(:get).with('/teakflake-servers')
        allow(zookeeper).to receive(:children).with('/teakflake-servers').and_return([(worker_id - 2).to_s, (worker_id - 1).to_s])
        allow(zookeeper).to receive(:get).with("/teakflake-servers/#{worker_id - 2}").and_return(peer0)
        allow(zookeeper).to receive(:get).with("/teakflake-servers/#{worker_id - 1}").and_return(peer1)
        stub_request(:post, "#{peer0}/id").and_return(body: JSON.generate({ metadata: {}, response: { ids: [id0] } }))
        stub_request(:post, "#{peer1}/id").and_return(body: JSON.generate({ metadata: {}, response: { ids: [id1] } }))
        allow(clock).to receive(:millis).and_return(52_000 + Teakflake::Id::EPOCH)
      end

      it 'raises an error' do
        expect { id_assigner.sanity_check_peers }.to raise_error('datacenter id insanity')
      end

      it 'logs an error' do
        id_assigner.sanity_check_peers rescue nil
        expect(@log_messages).to include(
          contain_exactly(:error, :datacenter_id_insanity, hash_including(expected: datacenter_id, got: datacenter_id - 1))
        )
      end
    end

    context 'with timestamp insanity' do
      let(:worker_id) { rand(Teakflake::Id::MAX_WORKER_ID - 2) + 2 }
      let(:datacenter_id) { rand(Teakflake::Id::MAX_DATACENTER_ID - 2) + 2 }
      let(:peer0) { 'http://peer0:80' }
      let(:peer1) { 'http://peer1:80' }
      let(:id0) { Teakflake::Id.from_parts(66_000, datacenter_id, worker_id - 2, 0).id }
      let(:id1) { Teakflake::Id.from_parts(66_000, datacenter_id, worker_id - 1, 0).id }

      before do
        allow(zookeeper).to receive(:get).with('/teakflake-servers')
        allow(zookeeper).to receive(:children).with('/teakflake-servers').and_return([(worker_id - 2).to_s, (worker_id - 1).to_s])
        allow(zookeeper).to receive(:get).with("/teakflake-servers/#{worker_id - 2}").and_return(peer0)
        allow(zookeeper).to receive(:get).with("/teakflake-servers/#{worker_id - 1}").and_return(peer1)
        stub_request(:post, "#{peer0}/id").and_return(body: JSON.generate({ metadata: {}, response: { ids: [id0] } }))
        stub_request(:post, "#{peer1}/id").and_return(body: JSON.generate({ metadata: {}, response: { ids: [id1] } }))
        allow(clock).to receive(:millis).and_return(52_000 + Teakflake::Id::EPOCH)
      end

      it 'raises an error' do
        expect { id_assigner.sanity_check_peers }.to raise_error('timestamp insanity')
      end

      it 'logs an error' do
        id_assigner.sanity_check_peers rescue nil
        expect(@log_messages).to include(
          contain_exactly(:error, :timestamp_insanity, hash_including(our_time: 52_000 + Teakflake::Id::EPOCH, avg: 66_000.0 + Teakflake::Id::EPOCH))
        )
      end
    end
  end
end
