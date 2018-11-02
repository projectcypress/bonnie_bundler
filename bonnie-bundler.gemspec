# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = "bonnie_bundler"
  s.summary = "A Gem for creating and managing bonnie bundles"
  s.description = "A Gem for creating and managing bonnie bundles"
  s.email = "pophealth-talk@googlegroups.com"
  s.homepage = "http://github.com/projecttacoma/bonnie_bundler"
  s.authors = ["The MITRE Corporation"]
  s.version = '2.2.1'
  s.license = 'Apache-2.0'

  s.add_dependency 'health-data-standards', '~> 4.2'
  s.add_dependency 'quality-measure-engine', '~> 3.1.2'
  s.add_dependency 'hquery-patient-api', '~> 1.1'
  s.add_dependency 'simplexml_parser', '~> 1.0'
  s.add_dependency 'hqmf2js', '~> 1.4'

  s.add_dependency 'rails', '~> 4.2'
  s.add_dependency 'mongoid', '~> 5.0'
  s.add_dependency 'rubyzip', '~> 1.2', '>= 1.2.1'
  s.add_dependency 'zip-zip', '~> 0.3'
  s.add_dependency 'diffy', '~> 3.0.0'

  # needed for parsing value sets (we need to use roo rather than rubyxl because the value sets are in xls rather than xlsx)
  s.add_dependency 'roo', '~> 1.13'

  s.files = s.files = `git ls-files`.split("\n")
end
