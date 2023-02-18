require "nokogiri"
require "pathname"

module Relaton
  module Cli
    class RelatonFile
      def initialize(source, options = {})
        @source = source
        @options = options
        @outdir = options.fetch(:outdir, nil)
        @outfile = options.fetch(:outfile, nil)
      end

      def extract
        extract_and_write_to_files
      end

      def concatenate
        concatenate_and_write_to_files
      end

      def split
        split_and_write_to_files
      end

      # Extract files
      #
      # This interface expect us to provide a source file / directory,
      # output directory and custom configuration options. Then it wll
      # extract Relaton XML file / files to output directory from the
      # source file / directory. During this process it will use custom
      # options when available.
      #
      # @param source [Dir] The source directory for files
      # @param outdir [Dir] The output directory for files
      # @param options [Hash] Options as hash key value pair
      #
      def self.extract(source, outdir, options = {})
        new(source, options.merge(outdir: outdir)).extract
      end

      # Concatenate files
      #
      # This interface expect us to provide a source directory, output
      # file and custom configuration options. Normally, this expect the
      # source directory to contain RXL fles, but it also converts any
      # YAML files to RXL and then finally combines those together.
      #
      # This interface also allow us to provdie options like title and
      # organization and then it usage those details to generate the
      # collection file.
      #
      # @param source [Dir] The source directory for files
      # @param output [String] The collection output file
      # @param options [Hash] Options as hash key value pair
      #
      def self.concatenate(source, outfile, options = {})
        new(source, options.merge(outfile: outfile)).concatenate
      end

      # Split collection
      #
      # This interface expects us to provide a Relaton Collection
      # file and also an output directory, then it will split that
      # collection into multiple files.
      #
      # By default it usages `rxl` extension for these new files,
      # but we can also customize that by providing the correct
      # one as `extension` option parameter.
      #
      # @param source [File] The source collection file
      # @param output [Dir] The output directory for files
      # @param options [Hash] Options as hash key value pair
      #
      def self.split(source, outdir = nil, options = {})
        new(source, options.merge(outdir: outdir)).split
      end

      private

      attr_reader :source, :options, :outdir, :outfile

      def bibcollection
        Bibcollection.new(
          title: options[:title],
          items: concatenate_files,
          doctype: options[:doctype],
          author: options[:organization],
        )
      end

      # @param document [String] XML
      # @param file [String, nil] path to file
      # @return [Nokogiri::XML::Document]
      def nokogiri_document(document, file = nil)
        document ||= File.read(file, encoding: "utf-8")
        Nokogiri.XML(document)
      end

      def select_source_files
        if File.file?(source)
          [source]
        else
          select_files_with("xml")
        end
      end

      def relaton_collection
        @relaton_collection ||= Bibcollection.from_xml(nokogiri_document(nil, source))
      end

      def extract_and_write_to_files # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
        select_source_files.each do |file|
          xml = nokogiri_document(nil, file)
          xml.remove_namespaces!

          if (bib = xml.at("//bibdata"))
            bib = nokogiri_document(bib.to_xml)
          elsif (rfc = xml.at("//rfc"))
            require "relaton_ietf/bibxml_parser"
            ietf = RelatonIetf::BibXMLParser.fetch_rfc rfc
            bib = nokogiri_document ietf.to_xml(bibdata: true)
          else
            next
          end

          bib.remove_namespaces!
          bib.root.add_namespace(nil, "xmlns")

          bibdata = Relaton::Bibdata.from_xml(bib.root)
          if bibdata
            build_bibdata_relaton(bibdata, file)

            write_to_file(bibdata.send(output_type), outdir, build_filename(file))
          end
        end
      end

      def concatenate_files
        xml_files = [convert_rxl_to_xml, convert_yamls_to_xml, convert_xml_to_xml]

        xml_files.flatten.reduce([]) do |mem, xml|
          doc = nokogiri_document(xml[:content])
          if (rfc = doc.at("/rfc"))
            require "relaton_ietf/bibxml_parser"
            ietf = RelatonIetf::BibXMLParser.fetch_rfc rfc
            d = nokogiri_document ietf.to_xml(bibdata: true)
            mem << bibdata_instance(d, xml[:file])
          elsif %w[bibitem bibdata].include? doc&.root&.name
            mem << bibdata_instance(doc, xml[:file])
          else mem
          end
        end
      end

      def concatenate_and_write_to_files
        write_to_file(bibcollection.send(output_type))
      end

      def split_and_write_to_files
        output_dir = outdir || build_dirname(source)

        relaton_collection.items.each do |content|
          name = build_filename(nil, content.docidentifier)
          find_available_bibrxl_file(name, output_dir, content)
          write_to_file(content.send(output_type), output_dir, name)
        end
      end

      def find_available_bibrxl_file(name, _ouputdir, content)
        if options[:extension] == "yaml" || options[:extension] == "yml"
          bib_rxl = Pathname.new([outdir, name].join("/")).sub_ext(".rxl")
          content.bib_rxl = bib_rxl.to_s if File.file?(bib_rxl)
        end
      end

      def output_type(ext = options[:extension])
        ext ||= File.extname(outfile)[1..-1] if outfile
        case ext
        when "rxl", "xml"
          :to_xml
        when "yml", "yaml"
          :to_yaml
        else
          puts "[relaton-cli] the given extension of '#{ext}' is "\
          "not supported. Use 'rxl'."
          :to_xml
        end
      end

      # @param document [Nokogiri::XML::Document]
      # @param file [String] path to file
      # @return [Relaton::Bibdata]
      def bibdata_instance(document, file)
        document = clean_nokogiri_document(document)
        bibdata = Relaton::Bibdata.from_xml document.root
        build_bibdata_relaton(bibdata, file) if bibdata

        bibdata
      end

      # @param bibdata [Relaton::Bibdata]
      # @param file [String] path to file
      def build_bibdata_relaton(bibdata, file)
        ["xml", "pdf", "doc", "html", "rxl", "txt"].each do |type|
          file = Pathname.new(file).sub_ext(".#{type}")
          bibdata.send("#{type}=", file.to_s) if File.file?(file)
        end
      end

      # Force a namespace otherwise Nokogiri won't parse.
      # The reason is we use Bibcollection's from_xml, but that one
      # has an xmlns. We don't want to change the code for bibdata
      # hence this hack #bibdata_doc.root['xmlns'] = "xmlns"
      #
      # @param document [Nokogiri::XML::Document]
      # @return [Nokogiri::XML::Document]
      def clean_nokogiri_document(document)
        document.remove_namespaces!
        document.root.add_namespace(nil, "xmlns")
        nokogiri_document(document.to_xml)
      end

      def convert_rxl_to_xml
        select_files_with("{rxl}").map do |file|
          { file: file, content: File.read(file, encoding: "utf-8") }
        end
      end

      def convert_yamls_to_xml
        select_files_with("yaml").map do |file|
          { file: file, content: YAMLConvertor.to_xml(file, write: false) }
        end
      end

      def convert_xml_to_xml
        select_files_with("{xml}").map do |file|
          { file: file, content: File.read(file, encoding: "utf-8") }
        end
      end

      def select_files_with(extension)
        files = File.join(source, "**", "*.#{extension}")
        Dir[files].reject { |file| File.directory?(file) }
      end

      def write_to_file(content, directory = nil, output_file = nil)
        file_with_dir = [directory, output_file || outfile].compact.join("/")
        File.open(file_with_dir, "w:utf-8") { |file| file.write(content) }
      end

      def build_dirname(filename)
        basename = File.basename(filename)&.gsub(/.(xml|rxl)/, "")
        directory_name = sanitize_string(basename)
        Dir.mkdir(directory_name) unless File.exists?(directory_name)

        directory_name
      end

      def build_filename(file, identifier = nil, ext = "rxl")
        identifier ||= Pathname.new(File.basename(file.to_s, ".xml")).to_s
        [sanitize_string(identifier), options[:extension] || ext].join(".")
      end

      def sanitize_string(string)
        clean_string = replace_bad_characters(string.downcase)
        clean_string.gsub(/^\s+/, "").gsub(/\s+$/, "").gsub(/\s+/, "-")
      end

      def replace_bad_characters(string)
        bad_chars = ["/", "\\", "?", "%", "*", ":", "|", '"', "<", ">", ".", " "]
        bad_chars.reduce(string.downcase) { |res, char| res.gsub(char, "-") }
      end
    end
  end
end
