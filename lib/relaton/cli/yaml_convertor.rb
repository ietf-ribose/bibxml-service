require "yaml"
require "relaton/cli/base_convertor"
require "relaton_bib"

module Relaton
  module Cli
    class YAMLConvertor < Relaton::Cli::BaseConvertor
      def to_xml
        if writable
          convert_and_write(file_content, :to_xml)
        else
          convert_content(file_content).to_xml date_format: :full, bibdata: true
        end
      end

      class << self
        # Convert to XML
        #
        # This interface allow us to convert any YAML file to XML.
        # It only require us to provide a valid YAML file and it can
        # do converstion using default attributes, but it also allow
        # us to provide custom options to customize this converstion
        # process.
        #
        # @param file [File] The complete path to a YAML file
        # @param options [Hash] Options as hash key, value pairs.
        #
        def to_xml(file, options = {})
          new(file, options).to_xml
        end

        # @param content [Hash] document in YAML format
        # @return [RelatonBib::BibliographicItem,
        #   RelatonIso::IsoBiblioraphicItem]
        def convert_single_file(content)
          if (processor = Registry.instance.by_type(doctype(content["docid"])))
            processor.hash_to_bib content
          else
            RelatonBib::BibliographicItem.new(
              **RelatonBib::HashConverter::hash_to_bib(content)
            )
          end
        end

        private

        # @param content [Hash]
        # @return [String]
        def doctype(docid)
          did = docid.is_a?(Array) ? docid.fetch(0) : docid
          return unless did

          did["type"] || did.fetch("id")&.match(/^\w+/)&.to_s
        end
      end

      private

      def default_ext
        "rxl"
      end

      def file_content
        date_to_string(YAML.load_file(file))
      end

      def date_to_string(obj)
        if obj.is_a? Hash
          obj.reduce({}) do |memo, (k, v)|
            memo[k] = date_to_string(v)
            memo
          end
        elsif obj.is_a? Array
          obj.reduce([]) { |memo, v| memo << date_to_string(v) }
        else
          obj.is_a?(Date) ? obj.to_s : obj
        end
      end

      def convert_collection(content)
        if content.has_key?("root")
          content["root"]["items"] = content["root"]["items"].map do |i|
            # RelatonBib::HashConverter::hash_to_bib(i)
            self.class.convert_single_file(i)
          end
          Relaton::Bibcollection.new(content["root"])
        end
      end

      def xml_content(_raw_file)
        convert_content(file_content).to_xml(date_format: :full, bibdata: true)
      end

      def convert_content(content)
        convert_collection(content) || self.class.convert_single_file(content)
      end
    end
  end
end
