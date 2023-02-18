require "fileutils"
require "relaton/bibdata"
require "relaton/bibcollection"
require "relaton/cli/xml_to_html_renderer"

module Relaton
  module Cli
    class BaseConvertor
      def initialize(file, options = {})
        @file = file
        @options = options
        @outdir = options.fetch(:outdir, nil)
        @writable = options.fetch(:write, true)
        @overwrite = options.fetch(:overwrite, true)
        @default_filelabel = 0

        install_dependencies(options[:require] || [])
      end

      # @return [String] HTML
      def to_html
        content = convert_to_html
        write_to_a_file(content)
      end

      # Convert to HTML
      #
      # This interface expect us to provide Relaton collection XML
      # as XML/RXL, and necessary styels / templates then it will be
      # used convert that collection to HTML.
      #
      # @param file [String] Relaton collection file path
      # @param style [String] Stylesheet file path for styles
      # @param template [String] The liquid tempalte directory
      #
      # @return [String] HTML
      def self.to_html(file, style = nil, template = nil)
        new(
          file,
          style: style || File.join(File.dirname(__FILE__), "../../../templates/index-style.css"),
          template: template || File.join(File.dirname(__FILE__), "../../../templates/"),
          extension: "html"
        ).to_html
      end

      private

      attr_reader :file, :outdir, :options, :writable, :overwrite

      # @return [String] HTML
      def convert_to_html
        Relaton::Cli::XmlToHtmlRenderer.render(
          xml_content(file),
          stylesheet: options[:style],
          liquid_dir: options[:template]
        )
      end

      # @param file [String] path to a file
      # @return [String] the file's content
      # @return [String] HTML
      def xml_content(file)
        File.read(file, encoding: "utf-8")
      end

      def install_dependencies(dependencies)
        dependencies.each { |dependency| require(dependency) }
      end

      def item_output(content, format)
        case format.to_sym
        when :to_yaml then content.to_yaml
        when :to_xml then content.to_xml(date_format: :full, bibdata: true)
        end
      end

      def convert_and_write(content, format)
        content = convert_content(content)
        write_to_a_file(item_output(content, format))
        write_to_file_collection(content, format.to_sym)
      end

      def write_to_a_file(content, outfile = nil)
        outfile ||= Pathname.new(file).sub_ext(extension).to_s

        if !File.exists?(outfile) || overwrite
          File.open(outfile, "w:utf-8") do |file|
            file.write(content)
          end
        end
      end

      def write_to_file_collection(content, format)
        if outdir && content.is_a?(Relaton::Bibcollection)
          FileUtils.mkdir_p(outdir)
          content.items_flattened.each do |item|
            collection = collection_filename(extract_docid(item))
            write_to_a_file(item_output(item, format), collection)
          end
        end
      end

      def extract_docid(item)
        @default_filelabel += 1
        item.docidentifier.nil? && (return @default_filelabel.to_s)
        # item.docidentifier.is_a?(Array) or return @default_filelabel.to_s
        item.docidentifier.empty? && (return @default_filelabel.to_s)
        docidentifier_code(item.docidentifier)
      end

      # From http://gavinmiller.io/2016/creating-a-secure-sanitization-function/
      FILENAME_BAD_CHARS = ["/", '\\', "?", '%", "*", ":', "|", '"', "<", ">",
                            ".", " "].freeze

      def docidentifier_code(docidentifier)
        return "" if docidentifier.nil?

        FILENAME_BAD_CHARS.reduce(docidentifier.downcase) do |result, bad_char|
          result.gsub(bad_char, "-")
        end
      end

      def extension
        @extension ||= [".", options.fetch(:extension, default_ext)].join
      end

      def collection_filename(identifier)
        File.join(
          outdir, [@options[:prefix], identifier, extension].compact.join("")
        )
      end
    end
  end
end
