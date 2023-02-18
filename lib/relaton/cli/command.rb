require "relaton/cli/relaton_file"
require "relaton/cli/xml_convertor"
require "relaton/cli/yaml_convertor"
require "relaton/cli/data_fetcher"
require "relaton/cli/subcommand_collection"
require "relaton/cli/subcommand_db"
require "fcntl"

module Relaton
  module Cli
    class Command < Thor
      include Relaton::Cli
      class_before :relaton_config
      class_option :verbose, aliases: :v, type: :boolean, desc: "Output warnings"

      desc "fetch CODE", "Fetch Relaton XML for Standard identifier CODE"
      option :type, aliases: :t, desc: "Type of standard to "\
                                       "get bibliographic entry for"
      option :format, aliases: :f, desc: "Output format (xml, yaml, bibtex). "\
                                         "Default xml."
      option :year, aliases: :y, type: :numeric, desc: "Year the standard was "\
                                                       "published"
      option :"all-parts", type: :boolean, desc: "Fetch all parts"
      option :"keep-year", type: :boolean, desc: "Undated reference should "\
                                                 "return actual reference with year"
      option :retries, aliases: :r, type: :numeric, desc: "Number of network "\
        "retries. Default 1."

      def fetch(code)
        io = IO.new($stdout.fcntl(::Fcntl::F_DUPFD), mode: "w:UTF-8")
        io.puts(fetch_document(code, options) || supported_type_message)
      end

      desc "extract Metanorma-XML-File / Directory Relaton-XML-Directory",
           "Extract Relaton XML from Metanorma XML file / directory"
      option :extension, aliases: :x, default: "rxl", desc: "File extension "\
        "of Relaton XML files, defaults to 'rxl'"

      def extract(source_dir, outdir)
        Relaton::Cli::RelatonFile.extract(source_dir, outdir, options)
      end

      desc "concatenate SOURCE-DIR COLLECTION-FILE", "Concatenate entries in "\
        "DIRECTORY (containing Relaton-XML or YAML) into a Relaton Collection"
      option :title, aliases: :t,  desc: "Title of resulting Relaton collection"
      option :organization, aliases: :g, desc: "Organization owner of Relaton "\
                                               "collection"
      option :extension, aliases: :x, desc: "File extension of destination "\
        "Relaton file, defaults to 'rxl'"

      def concatenate(source_dir, outfile)
        Relaton::Cli::RelatonFile.concatenate(source_dir, outfile, options)
      end

      desc "split Relaton-Collection-File Relaton-XML-Directory", "Split a "\
        "Relaton Collection into multiple files"
      option :extension, aliases: :x, default: "rxl", desc: "File extension "\
        "of Relaton XML files, defaults to 'rxl'"

      def split(source, outdir)
        Relaton::Cli::RelatonFile.split(source, outdir, options)
      end

      desc "yaml2xml YAML", "Convert Relaton YAML into Relaton Collection XML "\
                            "or separate files"
      option :extension, aliases: :x, default: "rxl", desc: "File extension "\
        "of Relaton XML files, defaults to 'rxl'"
      option :prefix, aliases: :p, desc: "Filename prefix of individual "\
                                         "Relaton XML files, defaults to empty"
      option :outdir, aliases: :o, desc: "Output to the specified directory "\
        "with individual Relaton Bibdata XML files"
      option :require, aliases: :r, type: :array, desc: "Require LIBRARY "\
                                                        "prior to execution"
      option :overwrite, aliases: :f, type: :boolean, default: false,
                         desc: "Overwrite the existing file"

      def yaml2xml(filename)
        Relaton::Cli::YAMLConvertor.to_xml(filename, options)
      end

      desc "xml2yaml XML", "Convert Relaton XML into Relaton Bibdata / "\
                           "Bibcollection YAML (and separate files)"
      option :extension, aliases: :x, default: "yaml", desc: "File extension "\
        "of Relaton YAML files, defaults to 'yaml'"
      option :prefix, aliases: :p, desc: "Filename prefix of Relaton XML "\
                                         "files, defaults to empty"
      option :outdir, aliases: :o, desc: "Output to the specified directory "\
                                         "with individual Relaton Bibdata YAML files"
      option :require, aliases: :r, type: :array, desc: "Require LIBRARY "\
                                                        "prior to execution"
      option :overwrite, aliases: :f, type: :boolean, default: false,
                         desc: "Overwrite the existing file"

      def xml2yaml(filename)
        Relaton::Cli::XMLConvertor.to_yaml(filename, options)
      end

      desc "xml2html RELATON-INDEX-XML", "Convert Relaton Collection XML into "\
                                         "HTML"
      option :stylesheet, aliases: :s, desc: "Stylesheet file path for "\
                                             "rendering HTML index"
      option :templatedir, aliases: :t, desc: "Liquid template directory for "\
        "rendering Relaton items and collection"
      option :overwrite, aliases: :f, type: :boolean, default: false,
                         desc: "Overwrite the existing file"

      def xml2html(file, style = nil, template = nil)
        Relaton::Cli::XMLConvertor.to_html(file, style, template)
      end

      desc "yaml2html RELATON-INDEX-YAML", "Concatenate Relaton Collection "\
                                           "YAML into HTML"
      option :stylesheet, aliases: :s, desc: "Stylesheet file path for "\
                                             "rendering HTML index"
      option :templatedir, aliases: :t, desc: "Liquid template directory for "\
        "rendering Relaton items and collection"
      option :overwrite, aliases: :f, type: :boolean, default: false,
                         desc: "Overwrite the existing file"

      def yaml2html(file, style = nil, template = nil)
        Relaton::Cli::YAMLConvertor.to_html(file, style, template)
      end

      desc "convert XML", "Convert Relaton XML document"
      option :format, aliases: :f, required: true, desc: "Output format "\
        "(yaml, bibtex, asciibib)"
      option :output, aliases: :o, desc: "Output to the specified file"

      def convert(file) # rubocop:disable Metrics/AbcSize,Metrics/MethodLength
        xml = Nokogiri::XML(File.read(file, encoding: "UTF-8"))
        item = Relaton::Cli.parse_xml xml
        result = if /yaml|yml/.match?(options[:format])
                   item.to_hash.to_yaml
                 else item.send "to_#{options[:format]}"
                 end
        ext = case options[:format]
              when "bibtex" then "bib"
              when "asciibib" then "adoc"
              else options[:format]
              end
        output = options[:output] || file.sub(/(?<=\.)[^.]+$/, ext)
        File.write output, result, encoding: "UTF-8"
      end

      desc "fetch-data SOURCE", "Fetch all the documents from a source"
      option :output, aliases: :o, desc: "Output dir. Default: ./data/"
      option :format, aliases: :f, desc: "Output format (yaml, xml, bibxml). Default: yaml"

      def fetch_data(source)
        DataFetcher.fetch source, options
      end

      desc "collection SUBCOMMAND", "Collection manipulations"
      subcommand "collection", SubcommandCollection

      desc "db SUBCOMMAND", "Cache DB manipulation"
      subcommand "db", SubcommandDb

      no_commands do
        def relaton_config
          log_types = %i[info error]
          log_types << :warning if options[:verbose]
          Relaton.configure do |conf|
            conf.logs = log_types
          end
        end
      end
    end

    private

    # @param code [String]
    # @param options [Hash]
    # @option options [String] :type
    # @option options [String, NilClass] :format
    # @option options [Integer, NilClass] :year
    # @return [String, nil]
    def fetch_document(code, options) # rubocop:disable Metrics/CyclomaticComplexity,Metrics/PerceivedComplexity,Metrics/AbcSize
      year = options[:year]&.to_s
      if (processor = Relaton::Registry.instance.by_type options[:type]&.upcase)
        doc = Relaton.db.fetch_std code, year, processor.short, options.dup
      elsif options[:type] then return
      else doc = Relaton.db.fetch(code, year, options.dup)
      end
      return "No matching bibliographic entry found" unless doc

      serialize doc, options[:format]
    rescue RelatonBib::RequestError => e
      e.message
    end

    # @param doc [RelatonBib::BibliographicItem]
    # @param format [String]
    # @return [String]
    def serialize(doc, format)
      case format
      when "yaml", "yml" then doc.to_hash.to_yaml
      when "bibtex" then doc.to_bibtex
      else doc.to_xml bibdata: true
      end
    end

    def supported_type_message
      ["Recognised types:", registered_types.sort.join(", ")].join(" ")
    end

    def registered_types
      @registered_types ||=
        Relaton::Registry.instance.processors.each.map { |_n, pr| pr.prefix }
    end
  end
end
