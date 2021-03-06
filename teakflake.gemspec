# frozen_string_literal: true

require_relative "lib/teakflake/version"

Gem::Specification.new do |spec|
  spec.name          = "teakflake"
  spec.version       = Teakflake::VERSION
  spec.authors       = ["Alex Scarborough"]
  spec.email         = ["alex@teak.io"]

  spec.summary       = "Generate Twitter Snowflake style ids."
  spec.homepage      = "https://github.com/GoCarrot/teakflake"
  spec.required_ruby_version = Gem::Requirement.new(">= 2.7.0")

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/GoCarrot/teakflake"
  spec.metadata["changelog_uri"] = "https://github.com/GoCarrot/teakflake/blob/main/CHANGELOG.md"
  spec.license = 'Apache-2.0'

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{\A(?:test|spec|features)/}) }
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency 'logsformyfamily', '~> 0.3.0'
  spec.add_dependency 'zk', '~> 1.10'
  spec.add_dependency 'rack', '~> 2.2', '>= 2.2.3.1'
end
