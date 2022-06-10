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

require 'json'
require 'zk'
require 'yaml'
require 'securerandom'
require_relative 'clocks/process'
require_relative 'worker_id_sources/static_worker_id'
require_relative 'id_worker'
require_relative 'version'

module Teakflake
  class FileLogWriter
    def initialize(file_path)
      @file_path = file_path
    end

    def call(level_name, event_type, merged_data)
      merged_data[:level] = level_name
      merged_data[:event_type] = event_type
      File.open(@file_path, 'a') { |f| f.write("#{JSON.generate(merged_data)}\n") }
    end
  end

  class Server
    def config
      @config ||= {}.freeze
    end

    def load_application_config!
      path = File.join(ENV.fetch('CREDENTIALS_DIRECTORY', 'config'), 'application.yml')
      if File.exists?(path)
        yaml = File.read(path)
        parsed_config = YAML.safe_load(yaml, symbolize_names: true).freeze
        if(parsed_config)
          @config = parsed_config
          parsed_config.each do |(key, value)|
            str_key = key.to_s
            env_key = str_key.upcase
            # Only set ENV_VAR_LIKE config keys in env.
            ENV[env_key] = value.to_s if env_key == str_key
          end
        end
      end
    end

    def initialize
      @hostname = Socket.gethostname
      load_application_config!
      LogsForMyFamily.configuration.backends = [FileLogWriter.new("#{ENV['LOGS_DIRECTORY']}/teakflake.log")]
      id_assigner = StaticWorkerId.new(
        ZK::Client::Threaded.new(config[:zookeeper_servers].join(',')),
        config[:datacenter_id],
        ENV['WORKER_ID'].to_i,
        "http://#{Addrinfo.getaddrinfo(@hostname, nil, nil, :STREAM,nil, Socket::AI_CANONNAME).first.canonname}:#{config[:server_port]}",
        ProcessClock.new
      )
      id_assigner.sanity_check_peers
      id_assigner.register_worker_id
      @id_worker = IdWorker.new(id_assigner)
    end

    def call(env)
      parse_json(env)
      case env['PATH_INFO']
      when '/id'
        get_id(env)
      when '/metadata'
        get_metadata(env)
      else
        get_404(env)
      end
    end

  private

    POST_BODY = 'rack.input'.freeze
    FORM_INPUT = 'rack.request.form_input'.freeze
    FORM_HASH = 'rack.request.form_hash'.freeze

    def parse_json(env)
      post_body = env[POST_BODY]
      body = post_body.read
      return if body.empty?

      post_body.rewind
      env.update(
        FORM_HASH => JSON.parse(body), FORM_INPUT => post_body
      )
    end

    def get_metadata(env)
      start_time = Time.now.to_f
      request_id = env['HTTP_X_AMZN_TRACE_ID'] || SecureRandom.uuid
      server_id = "#{@hostname}-#{$$}"
      [200, { 'Content-Type' => 'application/json' }, [
        JSON.generate({metadata: {
          requestId: request_id,
          requestTime: (Time.now.to_f - start_time) * 1000,
          serverId: server_id,
          version: ::Teakflake::VERSION
        }, response: {}})
      ]]
    end

    def get_404(env)
      start_time = Time.now.to_f
      request_id = env['HTTP_X_AMZN_TRACE_ID'] || SecureRandom.uuid
      server_id = "#{@hostname}-#{$$}"
      [404, { 'Content-Type' => 'application/json' }, [
        JSON.generate({metadata: {
          requestId: request_id,
          requestTime: (Time.now.to_f - start_time) * 1000,
          serverId: server_id,
          version: ::Teakflake::VERSION
        }, error: "#{env['PATH_INFO']} not found" })
      ]]
    end

    def get_id(env)
      start_time = Time.now.to_f
      request_id = env['HTTP_X_AMZN_TRACE_ID'] || SecureRandom.uuid
      server_id = "#{@hostname}-#{$$}"
      count = (env[FORM_HASH] || {}).fetch('count', 1).to_i
      ids = @id_worker.id(count)
      [200, { 'Content-Type' => 'application/json' }, [
        JSON.generate({metadata: {
          requestId: request_id,
          requestTime: (Time.now.to_f - start_time) * 1000,
          serverId: server_id,
          version: ::Teakflake::VERSION
        }, response: { ids: ids }})
      ]]
    rescue StandardError => exc
      [500, { 'Content-Type' => 'application/json' }, [
        JSON.generate({metadata: {
          requestId: request_id,
          requestTime: (Time.now.to_f - start_time) * 1000,
          serverId: server_id,
          version: ::Teakflake::VERSION
        }, error: exc.message })
      ]]
    end
  end
end
