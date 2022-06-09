# frozen_string_literal: true

require 'teakflake/id'

RSpec.describe Teakflake::Id do
  describe '.from_parts' do
    let(:timestamp) { rand(50_000) }
    let(:worker_id) { rand(Teakflake::Id::MAX_WORKER_ID) }
    let(:datacenter_id) { rand(Teakflake::Id::MAX_DATACENTER_ID) }
    let(:sequence) { rand(Teakflake::Id::MAX_SEQUENCE) }

    subject(:id) { described_class.from_parts(timestamp, datacenter_id, worker_id, sequence) }

    it 'creates a proper id' do
      expect(id).to have_attributes(
        timestamp: timestamp + Teakflake::Id::EPOCH,
        worker_id: worker_id,
        datacenter_id: datacenter_id,
        sequence: sequence
      )
    end
  end
end
