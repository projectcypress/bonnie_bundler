require 'cqm/converter'

module Measures
  module Exporter
    class BundleExporter

      attr_accessor :cql_measures
      attr_accessor :config
      attr_accessor :records
      
      Mongoid.load!('config/mongoid.yml', :development)

      DEFAULTS = {"library_path" => "library_functions",
                  "measures_path" => "measures",
                  "sources_path" => "sources",
                  "records_path" => "patients",
                  "results_path" => "results",
                  "valuesets_path" => "value_sets",
                  "base_dir" => "./bundle",
                  "hqmf_path" => "db/measures",
                  "enable_logging" => false,
                  "enable_rationale" =>false,
                  "short_circuit"=>false,
                  "effective_date" => Measure::DEFAULT_EFFECTIVE_DATE,
                  "name" =>"bundle-#{Time.now.to_i}",
                  "check_crosswalk" => false,
                  "use_cms" => false,
                  "export_filter" => ["measures", "sources","records", "valuesets", "results"]}
      
      DEFAULTS.keys.each do |k|
        attr_accessor k.to_sym
      end

      def initialize(user, config={})
        db = Mongoid::Clients.default
        # convert symbol keys to strings
        config = config.inject({}) { |memo,(key,value)| memo[key.to_s] = value; memo}
        @config = DEFAULTS.merge(config)
        @ep_patient_ids = []
        @eh_patient_ids = []
        @ep_measure_ids = []
        @eh_measure_ids = []
        @vs_to_export = []
        @ep_user = User.where(email: @config['users']['ep']).first
        @eh_user = User.where(email: @config['users']['eh']).first
        @user = user
        @cql_measures = CqlMeasure.where(user_id: @ep_user.id) + CqlMeasure.where(user_id: @eh_user.id)
        @records = @ep_user.records + @eh_user.records
        @hds_record_converter = CQM::Converter::HDSRecord.new
        DEFAULTS.keys.each do |name|
          instance_variable_set("@#{name}", @config[name])
        end
      end

      def export
        # remove all previous QDM patients
        QDM::Patient.destroy_all
        HealthDataStandards::CQM::Measure.destroy_all
        export_measures if export_filter.index("measures")
        export_patients if export_filter.index("records")
        export_valuesets if export_filter.index("valuesets")
        export_results if export_filter.index("results")
        export_file "bundle.json", bundle_json.to_json
      end


      def export_patients
        BonnieBundler.logger.info("Exporting patients")
        records.each_with_index do |patient, index|
          #next unless patient.first == 'B' && patient.last == 'GP Adult'
          safe_first_name = patient.first.gsub("'", "")
          safe_last_name = patient.last.gsub("'", "")
          filename =  "#{safe_first_name}_#{safe_last_name}"
          BonnieBundler.logger.info("Exporting patient #{filename}")
          BonnieBundler.logger.info("Exporting patient #{index}")

          record_with_sa_dose = false

          patient.source_data_criteria.each do |sdc|
            record_with_sa_dose = true if sdc['definition'] == 'substance' && sdc['status'] == 'administered'
            sdc['dose_unit'] = '{tablet(s)}' if sdc['dose_unit'] == 'tablet(s)'
            sdc['dose_unit'] = '{capsule(s)}' if sdc['dose_unit'] == 'capsule(s)'
            sdc['dose_unit'] = '{unit(s)}' if sdc['dose_unit'] == 'unit(s)'
            sdc['dose_unit'] = '{dose(s)}' if sdc['dose_unit'] == 'dose(s)'
            sdc['dose_unit'] = '{mcg(s)}' if sdc['dose_unit'] == 'mcg(s)'
            sdc['dose_unit'] = '{ml(s)}' if sdc['dose_unit'] == 'ml(s)'
          end
          patient.medications.each do |med|
            med['dose']['unit'] = '{tablet(s)}' if med['dose'] &&  med['dose']['unit'] == 'tablet(s)'
            med['dose']['unit'] = '{capsule(s)}' if med['dose'] &&  med['dose']['unit'] == 'capsule(s)'
            med['dose']['unit'] = '{unit(s)}' if med['dose'] &&  med['dose']['unit'] == 'unit(s)'
            med['dose']['unit'] = '{dose(s)}' if med['dose'] &&  med['dose']['unit'] == 'dose(s)'
            med['dose']['unit'] = '{mcg(s)}' if med['dose'] &&  med['dose']['unit'] == 'mcg(s)'
            med['dose']['unit'] = '{ml(s)}' if med['dose'] &&  med['dose']['unit'] == 'ml(s)'
          end
          # We should programattically swap UCUM units (dose(s) to {dose{s)}})
          qdm_patient = @hds_record_converter.to_qdm(patient)
          qdm_patient.extendedData['master_patient_id'] = qdm_patient._id.to_s

          if record_with_sa_dose == true
            qdm_patient.dataElements.each_with_index do |de, index|
              if de._type == 'QDM::SubstanceAdministered'
                de.dosage = QDM::Quantity.new(qdm_patient['extendedData']['source_data_criteria'][index]['field_values']['DOSE']['value'],qdm_patient['extendedData']['source_data_criteria'][index]['field_values']['DOSE']['unit'])
              end
            end
          end

          qdm_patient.dataElements.each do |de|
            if de['relatedTo']
              ref_array = []
              de.relatedTo.each do |ref|
                sdc = patient.source_data_criteria.clone
                sdc.keep_if { |sdc| sdc['coded_entry_id'].to_s == ref['value'] }
                ref_sdc = sdc.first
                ref_sdc_hash = { codes: ref_sdc['codes'].values.flatten, start_time: ref_sdc['start_date']/1000 + 157852800 }
                #qdm_sdc = { codes: qdm_patient.dataElements[7].dataElementCodes.map { |dec| dec['code']}, start_time: qdm_patient.dataElements[7][:authorDatetime] }
                ref_array << ref_sdc_hash
              end
              de.relatedTo = ref_array
              de.relatedTo = nil if de.relatedTo.blank? 
            end
          end

          ip = {}
          ip['financial_responsibility_type'] = { 'code' => 'SELF', 'codeSystem' => 'HL7 Relationship Code' }
          ip['codes'] = { 'SOP' => ['349'] }
          ip['name'] = 'Other'
          ip['member_id'] = Faker::Number.number(10)
          ip['start_time'] = qdm_patient.birthDatetime
          qdm_patient.extendedData['insurance_providers'] = JSON.generate([ip])
          qdm_patient.shift_dates(157852800)
          qdm_patient.save

          patient.user == @eh_user ? @eh_patient_ids << qdm_patient.id : @ep_patient_ids << qdm_patient.id

          patient_hash = qdm_patient.as_json(except: [ '_id', 'measure_id', 'bundle_id', 'bundleId', 'user_id' ], methods: ['_type'])
          patient_hash['measure_ids'] = patient_hash['measure_ids'].uniq if patient_hash['measure_ids']
          json = JSON.pretty_generate(JSON.parse(patient_hash.remove_nils.to_json))
          ##patient_type = patient.type || Measure.for_patient(patient).first.try(:type)
          path = File.join(records_path, patient.user == @eh_user ? 'eh' : 'ep')
          # In case 2 patients have the same name
          export_file File.join(path, "json", "#{filename}.json"), json
        end
      end

      def export_results
        BonnieBundler.logger.info("Exporting results")
        db = Mongoid::Clients.default
        db['qdm_individual_results'].delete_many({})

        options = { 'effective_date': Time.at(1514764799).in_time_zone.to_formatted_s(:number) }
        calc = Measures::Exporter::JsEcqmCalc.new(options)

        @eh_measure_ids.each do |mes_id|
          calc.sync_job(@eh_patient_ids, [mes_id])
        end
        @ep_measure_ids.each do |mes_id|
          calc.sync_job(@ep_patient_ids, [mes_id])
        end

        sleep(5)
        results_by_patient = db['qdm_individual_results'].find({ 'IPP' => { '$gt' => 0 }}).to_a
        results_by_patient = JSON.pretty_generate(JSON.parse(results_by_patient.as_json(:except => [ '_id']).to_json))
        
        export_file File.join(results_path,"by_patient.json"), results_by_patient
      end

      def export_valuesets
        BonnieBundler.logger.info("Exporting valuesets")
        value_sets = @vs_to_export.uniq
        HealthDataStandards::SVS::ValueSet.find(value_sets).each do |vs|
          export_file File.join(valuesets_path,"json", "#{vs.oid}-#{vs.version}.json"), JSON.pretty_generate(vs.as_json(:except => [ '_id', 'user_id' ]), max_nesting: 250)
        end
      end

      def export_measures
        BonnieBundler.logger.info("Exporting measures")
        @cql_measures.each do |measure|
          next unless @config[measure.hqmf_set_id]
          sub_ids = ('a'..'az').to_a
          if @config[measure.hqmf_set_id]
            measure['category'] = @config[measure.hqmf_set_id]['category']
            measure['measure_id'] = @config[measure.hqmf_set_id]['nqf_id']
          end
          dcab = Cypress::DataCriteriaAttributeBuilder.new
          dcab.build_data_criteria_for_measure(measure)
          measure.populations.each_with_index do |population, population_index|
            sub_id = sub_ids[population_index] if measure.populations.length > 1
            BonnieBundler.logger.info("Exporting measure #{measure.cms_id} - #{sub_id}")
            measure_json = measure.measure_json(population_index)
            # we clone so that we have a source without a bundle id
            temp_measure = measure_json.clone
            value_sets = []
            temp_measure[:value_set_oid_version_objects].each do |vsv|
              valueset_id = HealthDataStandards::SVS::ValueSet.where(:oid => vsv[:oid], :version => vsv[:version]).first.id
              value_sets << valueset_id
              @vs_to_export << valueset_id
            end
            temp_measure[:value_sets] = value_sets
            mes = Mongoid.default_client["measures"].insert_one(temp_measure)

            measure.user == @eh_user ? @eh_measure_ids << mes.inserted_id : @ep_measure_ids << mes.inserted_id

            measure_json['bonnie_measure_id'] = mes.inserted_id
            measure_json = JSON.pretty_generate(measure_json, max_nesting: 250)
            filename = "#{(config['use_cms'] ? measure.cms_id : measure.hqmf_id)}#{sub_id}.json"
            export_file File.join(measures_path, measure.type, filename), measure_json
          end

        end
      end

      def export_file(file_name, data)
        if @zip
          @zip.put_next_entry file_name
          @zip.puts data
        else
          write_to_file(file_name, data)
        end
      end

      def write_to_file(file_name, data)
        FileUtils.mkdir_p base_dir
        w_file_name = File.join(base_dir,file_name)
        FileUtils.mkdir_p File.dirname(w_file_name)
        FileUtils.remove_file(w_file_name,true)
        File.open(w_file_name,"w") do |f|
          f.puts data
        end
      end

      def compress_artifacts
        BonnieBundler.logger.info("compressing artifacts")
        zipfile_name = config["name"] 
         Zip::ZipFile.open("#{zipfile_name}.zip",  Zip::ZipFile::CREATE) do |zipfile|
          Dir[File.join(base_dir, '**', '**')].each do |file|
             fname = file.sub(base_dir, '')
             if fname[0] == '/'
                fname = fname.slice(1,fname.length)
              end
             zipfile.add(fname, file)
           end
        end
        zipfile_name
      end


      def bundle_json
        json = {
          title: config['title'],
          measure_period_start: config['measure_period_start'],
          effective_date: config['effective_date'],
          active: true,
          bundle_format: '3.0.0',
          smoking_gun_capable: true,
          version: config['version'],
          hqmfjs_libraries_version: config['hqmfjs_libraries_version'] || '1.0.0',
          license: config['license'],
          measures: @cql_measures.collect { |mes| mes['hqmf_id'] }.uniq,
          patients: records.collect { |rec| rec['medical_record_number'] }.uniq,
          exported: Time.now.strftime("%Y-%m-%d"),
        }
      end

    end   
  end
end
