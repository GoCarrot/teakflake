# frozen_string_literal: true

require 'teakflake/id_worker'
require 'teakflake/clocks/process'
require 'teakflake/id'

RSpec.describe Teakflake::IdWorker do
  StubWorkerId = Struct.new(:datacenter_id, :worker_id) do
    def assert(time)
      worker_id
    end
  end

  let(:datacenter_id) { rand(Teakflake::Id::MAX_DATACENTER_ID) }
  let(:worker_id) { rand (Teakflake::Id::MAX_WORKER_ID) }
  let(:worker_id_assigner) { StubWorkerId.new(datacenter_id, worker_id) }
  let(:clock) { instance_double(Teakflake::ProcessClock) }

  before do
    allow(clock).to receive(:millis).and_return(Teakflake::Id::EPOCH + 100)
  end

  subject(:id_worker) { described_class.new(worker_id_assigner, clock) }

  describe '#id' do
    it 'creates an id with the appropriate datacenter id, worker id, and timestamp' do
      id = Teakflake::Id.new(id_worker.id[0])
      expect(id).to have_attributes(
        worker_id: worker_id,
        datacenter_id: datacenter_id,
        timestamp: Teakflake::Id::EPOCH + 100
      )
    end

    it 'creats ids with different sequence numbers' do
      id0 = Teakflake::Id.new(id_worker.id[0])
      id1 = Teakflake::Id.new(id_worker.id[0])
      expect(id1.sequence).to be > id0.sequence
    end

    context 'when time goes backwards' do
      it 'raises a BackwardsTimeError' do
        id0 = Teakflake::Id.new(id_worker.id[0])
        allow(clock).to receive(:millis).and_return(Teakflake::Id::EPOCH + 99)
        expect { id_worker.id }.to raise_error(Teakflake::IdWorker::BackwardsTimeError)
      end
    end

    context 'when requested_count exceeds available ids' do
      it 'generates up to the available sequence' do
        ids = id_worker.id(Teakflake::Id::MAX_SEQUENCE / 2)
        more_ids = id_worker.id(Teakflake::Id::MAX_SEQUENCE)
        expect(more_ids.length).to eq(Teakflake::Id::MAX_SEQUENCE / 2)
      end
    end

    context 'when out of ids for the sequence' do
      before do
        id_worker.id(Teakflake::Id::MAX_SEQUENCE).length
        allow(clock).to receive(:millis).and_return(Teakflake::Id::EPOCH + 100, Teakflake::Id::EPOCH + 100, Teakflake::Id::EPOCH + 101)
      end

      it 'waits for the next millisecond before generating ids' do
        id = Teakflake::Id.new(id_worker.id.first)
        expect(clock).to have_received(:millis).exactly(4).times
      end

      it 'generates an id in the next millisecond' do
        id = Teakflake::Id.new(id_worker.id.first)
        expect(id).to have_attributes(timestamp: Teakflake::Id::EPOCH + 101)
      end

      context 'when time goes backwards' do
        before do
          allow(clock).to receive(:millis).and_return(Teakflake::Id::EPOCH + 100, Teakflake::Id::EPOCH + 100, Teakflake::Id::EPOCH + 99)
        end

        it 'raises a BackwardsTimeError' do
          expect { id_worker.id }.to raise_error(Teakflake::IdWorker::BackwardsTimeError)
        end
      end
    end
  end
end
