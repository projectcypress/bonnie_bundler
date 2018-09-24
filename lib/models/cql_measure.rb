class CqlMeasure
  include Mongoid::Document
  include Mongoid::Timestamps
  include Mongoid::Attributes::Dynamic

  DEFAULT_EFFECTIVE_DATE = Time.gm(2012,12,31,23,59).to_i
  MP_START_DATE = Time.gm(2012,1,1,0,0).to_i
  TYPES = ["ep", "eh"]

  field :id, type: String
  field :measure_id, type: String
  field :hqmf_id, type: String
  field :hqmf_set_id, type: String
  field :hqmf_version_number, type: Integer
  field :cms_id, type: String
  field :title, type: String, default: ""
  field :description, type: String, default: ""
  field :type, type: String
  field :category, type: String, default: 'uncategorized'

  field :episode_of_care, type: Boolean
  field :continuous_variable, type: Boolean
  field :calculate_sdes, type: Boolean
  field :episode_ids, type: Array

  field :needs_finalize, type: Boolean, default: false

  field :published, type: Boolean
  field :publish_date, type: Date
  field :version, type: Integer

  field :elm_annotations, type: Hash

  field :cql, type: Array
  field :elm, type: Array
  field :main_cql_library, type: String
  field :cql_statement_dependencies, type: Hash

  field :population_criteria, type: Hash
  field :data_criteria, type: Hash
  field :source_data_criteria, type: Hash
  field :measure_period, type: Hash
  field :measure_attributes, type: Array
  field :populations, type: Array
  field :populations_cql_map, type: Hash
  field :observations, type: Array

  field :value_set_oids, type: Array, default: []
  field :value_set_oid_version_objects, type: Array, default: []

  field :complexity, type: Hash

  belongs_to :user
  belongs_to :bundle, class_name: "HealthDataStandards::CQM::Bundle"
  has_and_belongs_to_many :records, :inverse_of => nil
  has_one :package, class_name: "CqlMeasurePackage", inverse_of: :measure, dependent: :delete

  scope :by_measure_id, ->(id) { where({'measure_id'=>id }) }
  scope :by_type, ->(type) { where({'type'=>type}) }
  scope :by_user, ->(user) { where user_id: user.id }

  index "user_id" => 1
  # Find the measures matching a patient
  def self.for_patient(record)
    where user_id: record.user_id, hqmf_set_id: { '$in' => record.measure_ids }
  end

  def value_sets
    options = { oid: value_set_oids }
    options[:user_id] = user.id if user?
    @value_sets ||= HealthDataStandards::SVS::ValueSet.in(options)
    @value_sets
  end

  def value_sets_by_oid
    @value_sets_by_oid = {}
    value_sets.each do |vs|
      if @value_sets_by_oid[vs.oid]
        # If there are multiple value sets with the same oid for the user, then keep the one with
        # the version corresponding to this measure.
        if vs.version.include?(hqmf_set_id)
          @value_sets_by_oid[vs.oid] = { vs.version => vs }
        end
      else
        @value_sets_by_oid[vs.oid] = { vs.version => vs }
      end
    end
    @value_sets_by_oid
  end

  # Returns the hqmf-parser's ruby implementation of an HQMF document.
  # Rebuild from population_criteria, data_criteria, and measure_period JSON
  def as_hqmf_model
    json = {
      "id" => self.measure_id,
      "title" => self.title,
      "description" => self.description,
      "population_criteria" => self.population_criteria,
      "data_criteria" => self.data_criteria,
      "source_data_criteria" => self.source_data_criteria,
      "measure_period" => self.measure_period,
      "attributes" => self.measure_attributes,
      "populations" => self.populations,
      "hqmf_id" => self.hqmf_id,
      "hqmf_set_id" => self.hqmf_set_id,
      "hqmf_version_number" => self.hqmf_version_number,
      "cms_id" => self.cms_id
    }
    HQMF::Document.from_json(json)
  end

  def all_data_criteria
    as_hqmf_model.all_data_criteria
  end

  def measure_json(population_index=0,check_crosswalk=false)
    hqmf_doc = self.as_hqmf_model
    hqmf_doc_json = self.as_hqmf_model.to_json
    options = {
      value_sets: value_sets,
      episode_ids: episode_ids,
      continuous_variable: continuous_variable,
      #force_sources: force_sources,
      #custom_functions: custom_functions,
      check_crosswalk: check_crosswalk
    }
    population_index ||= 0
    json = {
      id: self.hqmf_id,
      nqf_id: self.measure_id,
      hqmf_id: self.hqmf_id,
      hqmf_set_id: self.hqmf_set_id,
      hqmf_version_number: self.hqmf_version_number,
      cms_id: self.cms_id,
      name: self.title,
      description: self.description,
      type: self.type,
      category: self.category,
      source_data_criteria: hqmf_doc_json[:source_data_criteria],
      population_criteria: hqmf_doc_json[:population_criteria],
      data_criteria: hqmf_doc_json[:data_criteria],
      measure_attributes: hqmf_doc_json[:attributes],
      populations: hqmf_doc_json[:populations],
      measure_period: hqmf_doc_json[:measure_period],
      #map_fn: HQMF2JS::Generator::Execution.measure_js(self.as_hqmf_model, population_index, options),
      continuous_variable: self.continuous_variable,
      episode_of_care: self.episode_of_care,
      hqmf_document:  {:source_data_criteria => hqmf_doc_json[:source_data_criteria],
                       :data_criteria => hqmf_doc_json[:data_criteria]},
      elm_annotations: self.elm_annotations,
      observations: self.observations,
      cql: self.cql,
      elm: self.elm,
      main_cql_library: self.main_cql_library,
      cql_statement_dependencies: self.cql_statement_dependencies,
      populations_cql_map: self.populations_cql_map,
      value_set_oid_version_objects: self.value_set_oid_version_objects
    }
    if (self.populations.count > 1)
      sub_ids = ('a'..'az').to_a
      json[:sub_id] = sub_ids[population_index]
      population_title = self.populations[population_index]['title']
      json[:subtitle] = population_title
      json[:short_subtitle] = population_title
    end

    if self.continuous_variable
      observation = self.population_criteria[self.populations[population_index][HQMF::PopulationCriteria::OBSERV]]
      json[:aggregator] = observation['aggregator']
    end
    
    json[:oids] = self.value_sets.map{|value_set| value_set.oid}.uniq
    
    population_ids = {}
    HQMF::PopulationCriteria::ALL_POPULATION_CODES.each do |type|
      population_key = self.populations[population_index][type]
      population_criteria = self.population_criteria[population_key]
      if (population_criteria)
        population_ids[type] = population_criteria['hqmf_id']
      end
    end
    stratification = self['populations'][population_index]['stratification']
    if stratification
      population_ids['stratification'] = stratification 
    end
    json[:population_ids] = population_ids
    json
  end

  # Note whether or not the measure is a continuous variable measure.
  before_save :set_continuous_variable
  def set_continuous_variable
    # The return value of this function is not related to whether or not this
    # measure is a CV measure. The true return value ensures false is not
    # accidentally returned here, which would cause the chain of 'before_*' to
    # stop executing.
    self.continuous_variable = populations.map {|x| x.keys}.flatten.uniq.include? HQMF::PopulationCriteria::MSRPOPL
    true
  end

  # When saving calculate the cyclomatic complexity of the measure
  # TODO: Do we want to consider a measure other than "cyclomatic complexity" for CQL?
  # TODO: THIS IS NOT CYCLOMATIC COMPLEXITY, ALL MULTIPLE ELEMENT EXPRESSIONS GET COUNTED AS HIGHER COMPLEXITY, NOT JUST LOGICAL
  before_save :calculate_complexity
  def calculate_complexity
    # We calculate the complexity for each statement, and (at least for now) store the result in the same way
    # we store the complexity for QDM variables
    # TODO: consider whether this is too much of a force fit
    self.complexity = { variables: [] }
    # Recursively look through an expression to count the logical branches
    def count_expression_logical_branches(expression)
      case expression
      when nil
        0
      when Array
        expression.map { |exp| count_expression_logical_branches(exp) }.sum
      when Hash
        case expression['type']
        when 'And', 'Or', 'Not'
          count_expression_logical_branches(expression['operand'])
        when 'Query'
          # TODO: Do we need to look into the source side of the query? Can there be logical operators there?
          count_expression_logical_branches(expression['where']) + count_expression_logical_branches(expression['relationship'])
        else
          1
        end
      else
        0
      end
    end

    # Determine the complexity of each statement
    self.elm.each do |elm|
      if statements = elm.try(:[], 'library').try(:[], 'statements').try(:[], 'def')
        statements.each do |statement|
          self.complexity[:variables] << { name: statement['name'], complexity: count_expression_logical_branches(statement['expression']) }
        end
      end
    end
    self.complexity
  end

end

class CqlMeasurePackage
  include Mongoid::Document
  include Mongoid::Timestamps

  field :file, type: BSON::Binary
  belongs_to :measure, class_name: "CqlMeasure", inverse_of: :package
end
