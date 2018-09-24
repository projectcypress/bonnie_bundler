# Top level include file that brings in all the necessary code
require 'bundler/setup'
require 'rubygems'
require 'yaml'
require 'roo'

require 'quality-measure-engine'
require 'hqmf-parser'
require 'hqmf2js'
require 'simplexml_parser'
require 'active_support/core_ext/hash/indifferent_access'

require_relative 'models/measure.rb'
require_relative 'models/cql_measure.rb'
require_relative 'measures/exporter/data_criteria_attribute_builder.rb'
require_relative 'measures/exporter/bundle_exporter.rb'
require_relative 'measures/exporter/js_ecqm_calc.rb'
require_relative 'measures/loading/exceptions.rb'
require_relative 'measures/loading/loader.rb'
require_relative 'measures/loading/base_loader_definition.rb'
require_relative 'measures/loading/cql_loader.rb'
require_relative 'measures/loading/value_set_loader.rb'
require_relative 'measures/logic_extractor.rb'
require_relative 'measures/mongo_hash_key_wrapper.rb'
require_relative 'ext/hash.rb'
require_relative 'ext/valueset.rb'
require_relative 'measures/elm_parser.rb'
require_relative 'measures/cql_to_elm_helper.rb'
require_relative '../config/initializers/mongo.rb'
require_relative 'util/vsac_api.rb'

module BonnieBundler
  class << self
    attr_accessor :logger
  end
end

if defined?(Rails)
  require_relative 'ext/railtie'
else
  BonnieBundler.logger = Log4r::Logger.new("Bonnie Bundler")
  BonnieBundler.logger.outputters = Log4r::Outputter.stdout
end
