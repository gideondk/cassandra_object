module CassandraObject
  module Indexes
    extend ActiveSupport::Concern
    
    included do
      class_inheritable_accessor :indexes
    end
    
    class UniqueIndex
      def initialize(attribute_name, model_class, options)
        @attribute_name = attribute_name
        @model_class    = model_class
      end
      
      def find(attribute_value)
        # first find the key value
        key = @model_class.connection.get(column_family, attribute_value.to_s, 'key')
        # then pass to get
        if key
          @model_class.get(key.to_s)
        else
          @model_class.connection.remove(column_family, attribute_value.to_s)
          nil
        end
      end
      
      def write(record)
        @model_class.connection.insert(column_family, record.send(@attribute_name).to_s, {'key'=>record.key.to_s})
      end
      
      def remove(record)
        @model_class.connection.remove(column_family, record.send(@attribute_name).to_s)
      end
      
      def column_family
        @model_class.column_family + "By" + @attribute_name.to_s.camelize 
      end
      
      def column_family_configuration
        {:Name=>column_family, :CompareWith=>"UTF8Type"}
      end
    end
    
    class Index
      def initialize(attribute_name, model_class, options)
        @attribute_name = attribute_name
        @model_class    = model_class
        @reversed       = options[:reversed]
      end
      
      def find(*args)
        options = args.extract_options!
        attribute_value = args.join(":")
        
        cursor = CassandraObject::Cursor.new(@model_class, column_family, attribute_value.to_s, @attribute_name.to_s, :start_after=>options[:start_after], :reversed=>@reversed)
        cursor.validator do |object|
          object.send(@attribute_name) == attribute_value
        end
        cursor.find(options[:limit] || 100)
      end
      
      def write(record)
        @model_class.connection.insert(column_family, record.send(@attribute_name).to_s, {@attribute_name.to_s=>{new_key=>record.key.to_s}})
      end
      
      def remove(record)
      end
      
      def column_family
        @model_class.column_family + "By" + @attribute_name.to_s.camelize 
      end
      
      def new_key
        Cassandra::UUID.new
      end
      
      def column_family_configuration
        {:Name=>column_family, :CompareWith=>"UTF8Type", :ColumnType=>"Super", :CompareSubcolumnsWith=>"TimeUUIDType"}
      end
      
    end
    
    module ClassMethods
      def column_family_configuration
        if indexes
          super + indexes.values.map(&:column_family_configuration)
        else
          super
        end
      end
      
      def index(attribute_names, options = {})
        self.indexes ||= {}.with_indifferent_access
        
        attribute_names = Array(attribute_names)
        index_name = attribute_names.join("_")
        
        if options.delete(:unique)          
          self.indexes[index_name] = UniqueIndex.new(index_name, self, options)
          class_eval <<-eom
            def self.find_by_#{attribute_names.join("_and_")}(value)
              indexes[:#{index_name}].find(value)
            end
            
            after_save do |record|
              self.indexes[:#{index_name}].write(record)
              true
            end
              
            after_destroy do |record|
              record.class.indexes[:#{index_name}].remove(record)
              true
            end
          eom
        else
          self.indexes[index_name] = Index.new(index_name, self, options)
          class_eval <<-eom
            def self.find_all_by_#{attribute_names.join("_and_")}(*args)
              self.indexes[:#{index_name}].find(*args)
            end
            
            after_save do |record|
              record.class.indexes[:#{index_name}].write(record)
              true
            end
              
            after_destroy do |record|
              record.class.indexes[:#{index_name}].remove(record)
              true
            end
          eom
        end
      end
    end
  end
end