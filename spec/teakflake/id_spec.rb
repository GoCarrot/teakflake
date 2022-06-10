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
