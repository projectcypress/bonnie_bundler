source "https://rubygems.org"
gemspec

gem 'health-data-standards', :git => 'https://github.com/projectcypress/health-data-standards.git', :branch => 'master_bonnie'
# gem 'quality-measure-engine', :git => 'https://github.com/projectcypress/quality-measure-engine.git', :branch => 'master'
# gem 'hqmf2js', :git => 'https://github.com/projecttacoma/hqmf2js.git', :branch => 'master'
# gem 'hquery-patient-api', :git => 'https://github.com/projecttacoma/patientapi.git', :branch => 'master'
# gem 'simplexml_parser', :git => 'https://github.com/projecttacoma/simplexml_parser.git', :branch => 'master'

# gem 'health-data-standards', :path => '../health-data-standards'
# gem 'quality-measure-engine', :path => '../quality-measure-engine'
# gem 'hqmf2js', :path => '../hqmf2js'
# gem 'hquery-patient-api', :path => '../patientapi'
# gem 'simplexml_parser', :path => '../simplexml_parser'

group :development do
  gem 'bunny'
  gem 'rake'
  gem 'pry'
  gem 'pry-nav'
end

group :test do
  gem 'simplecov', :require => false
  gem 'minitest', "~> 5.0"
  gem 'awesome_print', :require => 'ap'
  gem 'vcr'
  gem 'webmock'
  gem 'bundler-audit'
end
