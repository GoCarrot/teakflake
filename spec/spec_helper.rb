# frozen_string_literal: true

require 'simplecov'

SimpleCov.start do
  enable_coverage :branch
end

require "teakflake"
require 'webmock/rspec'

RSpec.configure do |config|
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = ".rspec_status"

  # Disable RSpec exposing methods globally on `Module` and `main`
  config.disable_monkey_patching!

  config.order = :random

  # Seed global randomization in this process using the `--seed` CLI option.
  # Setting this allows you to use `--seed` to deterministically reproduce
  # test failures related to randomization by passing the same `--seed` value
  # as the one that triggered the failure.
  Kernel.srand config.seed

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  config.before do
    @log_messages = []
    LogsForMyFamily.configuration.backends = [
      proc do |level_name, event_type, merged_data|
        @log_messages << [level_name, event_type, merged_data]
      end
    ]
  end

  config.after do
    LogsForMyFamily.configuration.backends = []
  end

  WebMock.disable_net_connect!
end
