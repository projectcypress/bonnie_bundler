module Measures
  module Exporter
    class Calculator

      DEFAULTS = {'enable_logging' => false,
                  'enable_rationale' => false,
                  'check_crosswalk' => false,
                  'only_initialize' => false}


    # Refresh all JS libraries, refresh the bundle/measures collections, and calculate measures results.
      #
      # @param [Array] measures All the measures that will be calculated. Defaults to all measures.
      def self.calculate(config={}, measures = Measure.all)      
        refresh_js_libraries
        config = config.merge(DEFAULTS)
        only_initialize = config['only_initialize']
        check_crosswalk = config['check_crosswalk']
        # QME requires that the bundle collection be populated.
        MONGO_DB['bundles'].drop
        bundle = Measures::Exporter::BundleExporter.bundle_json([], [],config)
        MONGO_DB["bundles"].insert(JSON.parse(bundle.values.first))
        
        # Delete all old results for these measures because they might be out of date.
        MONGO_DB['query_cache'].where({'measure_id' => {'$in' => measures.map(&:hqmf_id)}}).remove_all unless only_initialize
        MONGO_DB['patient_cache'].where({'value.measure_id' => {'$in' => measures.map(&:hqmf_id)}}).remove_all unless only_initialize
        MONGO_DB['measures'].where({'hqmf_id' => {'$in' => measures.map(&:hqmf_id)}}).remove_all
        MONGO_DB.command({ getlasterror: 1 })
        
        # Break apart each measure into its submeasures and store as JSON into the measures collection for QME
        measures.each_with_index do |measure, measure_index|
          sub_ids = ['']
          sub_ids = ("a".."zz").to_a if measure.populations.count > 1
          measure.populations.each_with_index do |population, index|
            if (only_initialize)
              puts "rebuilding (#{measure_index+1}/#{measures.count}): #{measure.measure_id}#{sub_ids[index]}"
            else
              puts "calculating (#{measure_index+1}/#{measures.count}): #{measure.measure_id}#{sub_ids[index]}"
            end
            
            measure_json = Measures::Exporter::Calculator.measure_json(measure.measure_id, index, check_crosswalk)
            MONGO_DB["measures"].insert(measure_json)
            measure_id = MONGO_DB["measures"].find({id: measure_json[:id]}).first
            MONGO_DB["bundles"].find({}).update({"$push" => {"measures" => measure_id}})
            
            if !only_initialize
              effective_date = Measure::DEFAULT_EFFECTIVE_DATE
              oid_dictionary = HQMF2JS::Generator::CodesToJson.hash_to_js(Measures::Exporter::Calculator.measure_codes(measure))
              report = QME::QualityReport.new(measure_json[:id], measure_json[:sub_id], {'effective_date' => effective_date, 
                  'oid_dictionary' => oid_dictionary, 'enable_logging' => config['enable_logging'], "enable_rationale" =>config['enable_rationale']})
              report.calculate(false) unless report.calculated?
            end
          end
        end
      end

      def self.library_functions(check_crosswalk)
        library_functions = {}
        library_functions['map_reduce_utils'] = HQMF2JS::Generator::JS.map_reduce_utils
        library_functions['hqmf_utils'] = HQMF2JS::Generator::JS.library_functions(check_crosswalk)
        library_functions
      end    

      def self.refresh_js_libraries
        MONGO_DB['system.js'].find({}).remove_all
        library_functions.each do |name, contents|
          HealthDataStandards::Import::Bundle::Importer.save_system_js_fn(name, contents)
        end
      end

    def self.measure_json(measure_id, population_index ,check_crosswalk)
        population_index ||= 0
        
        measure = Measure.by_measure_id(measure_id).first
        buckets = measure.parameter_json(population_index, true)
        json = {
          id: measure.hqmf_id,
          nqf_id: measure.measure_id,
          hqmf_id: measure.hqmf_id,
          hqmf_set_id: measure.hqmf_set_id,
          hqmf_version_number: measure.hqmf_version_number,
          cms_id: measure.cms_id,
          endorser: measure.endorser,
          name: measure.title,
          description: measure.description,
          type: measure.type,
          category: measure.category,
          steward: measure.steward,
          population: buckets["population"],
          denominator: buckets["denominator"],
          numerator: buckets["numerator"],
          exclusions: buckets["exclusions"],
          map_fn: measure_js(measure, population_index,check_crosswalk),
          continuous_variable: measure.continuous_variable,
          episode_of_care: measure.episode_of_care,
          hqmf_document:  measure.as_hqmf_model.to_json
        }
        
        if (measure.populations.count > 1)
          sub_ids = ('a'..'az').to_a
          json[:sub_id] = sub_ids[population_index]
          population_title = measure.populations[population_index]['title']
          json[:subtitle] = population_title
          json[:short_subtitle] = population_title
          json[:hqmf_id] = measure.hqmf_id
          json[:hqmf_set_id] = measure.hqmf_set_id
          json[:hqmf_version_number] = measure.hqmf_version_number
        end

        if measure.continuous_variable
          observation = measure.population_criteria[measure.populations[population_index][HQMF::PopulationCriteria::OBSERV]]
          json[:aggregator] = observation['aggregator']
        end
        
        referenced_data_criteria = measure.as_hqmf_model.referenced_data_criteria
        json[:data_criteria] = referenced_data_criteria.map{|data_criteria| data_criteria.to_json}
        json[:oids] = measure.value_sets.map{|value_set| value_set.oid}.uniq
        
        population_ids = {}
        HQMF::PopulationCriteria::ALL_POPULATION_CODES.each do |type|
          population_key = measure.populations[population_index][type]
          population_criteria = measure.population_criteria[population_key]
          if (population_criteria)
            population_ids[type] = population_criteria['hqmf_id']
          end
        end
        stratification = measure['populations'][population_index]['stratification']
        if stratification
          population_ids['stratification'] = stratification 
        end
        json[:population_ids] = population_ids
        json
      end

      def self.measure_codes(measure)
        HQMF2JS::Generator::CodesToJson.from_value_sets(measure.value_sets)
      end

      private

      # Note that the JS returned by this function is not included when using the in-browser
      # debugger. See app/views/measures/debug.js.erb for the in-browser equivalent.
      def self.measure_js(measure, population_index, check_crosswalk)
        HQMF2JS::Generator::Execution.measure_js(measure, population_index, check_crosswalk)
      end
      
    end
  end
end