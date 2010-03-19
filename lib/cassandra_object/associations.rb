require 'cassandra_object/associations/one_to_many'
require 'cassandra_object/associations/one_to_one'

module CassandraObject
  module Associations
    extend ActiveSupport::Concern
    
    included do
      class_inheritable_hash :associations
      after_create :persist_associations
    end

    module ClassMethods
      def column_family_configuration
        super << {:Name=>"#{name}Relationships", :CompareWith=>"UTF8Type", :CompareSubcolumnsWith=>"TimeUUIDType", :ColumnType=>"Super"}
      end
      
      def association(association_name, options= {})
        if options[:unique]
          write_inheritable_hash(:associations, {association_name => OneToOneAssociation.new(association_name, self, options)})
        else
          write_inheritable_hash(:associations, {association_name => OneToManyAssociation.new(association_name, self, options)})
        end
      end
      
      def remove(key)
        begin
          connection.remove("#{name}Relationships", key.to_s)
        rescue Cassandra::AccessError => e
          raise e unless e.message =~ /Invalid column family/
        end
        super
      end
    end
    
    module InstanceMethods
      def persist_associations
        return true if self.class.associations.nil? || self.class.associations.empty?
        
        self.class.associations.each do |name, association|
          associated_object = self.instance_variable_get("@_#{name}".to_sym)
          association.set(self, associated_object) if associated_object
        end
      end
    end
  end
end