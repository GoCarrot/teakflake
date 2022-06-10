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

require 'rack/test'
require 'teakflake/server'
require 'tmpdir'

RSpec.describe Teakflake::Server do
  include Rack::Test::Methods

  let(:app) { Teakflake::Server.new }
  let(:zookeeper) { instance_double(ZK::Client::Threaded) }
  let(:worker_id) { rand(Teakflake::Id::MAX_WORKER_ID) }

  before do
    ENV['CREDENTIALS_DIRECTORY'] = 'spec/fixtures/server_test'
    ENV['LOGS_DIRECTORY'] = Dir.mktmpdir
    ENV['WORKER_ID'] = worker_id.to_s
    allow(ZK::Client::Threaded).to receive(:new).and_return(zookeeper)
    allow(zookeeper).to receive(:get).with('/teakflake-servers')
    allow(zookeeper).to receive(:mkdir_p)
    allow(zookeeper).to receive(:children).with('/teakflake-servers').and_return([])
    allow(zookeeper).to receive(:create)
  end

  after do
    FileUtils.remove_entry ENV['LOGS_DIRECTORY']
  end

  describe '/id' do
    it 'returns an id' do
      post '/id'
      body = JSON.parse(last_response.body)
      id = Teakflake::Id.new(body.dig('response', 'ids', 0))
      expect(id).to have_attributes(
        worker_id: worker_id,
        datacenter_id: 2,
        timestamp: be_within(2_000).of(Process.clock_gettime(Process::CLOCK_REALTIME, :millisecond))
      )
    end

    it 'has a 200 status' do
      post '/id'
      expect(last_response.status).to eq 200
    end

    it 'can request multiple ids' do
      post '/id', JSON.generate(count: 20)
      body = JSON.parse(last_response.body)
      expect(body.dig('response', 'ids').length).to eq 20
    end

    context 'with an error' do
      it 'reports the error' do
        post '/id', JSON.generate(count: -1)
        body = JSON.parse(last_response.body)
        expect(body['error']).to include('Must request at least one id')
      end

      it 'has a 500 status' do
        post '/id', JSON.generate(count: -1)
        expect(last_response.status).to eq 500
      end
    end
  end

  describe '/metadata' do
    it 'returns some server metdata' do
      get '/metadata'
      body = JSON.parse(last_response.body)
      expect(body['metadata'].keys).to include('requestId', 'requestTime', 'serverId', 'version')
    end

    it 'has a 200 status' do
      get '/metadata'
      expect(last_response.status).to eq 200
    end
  end

  describe 'a missing path' do
    it 'returns a 404' do
      get '/i_will_never_exist'
      expect(last_response.status).to eq 404
    end
  end
end
