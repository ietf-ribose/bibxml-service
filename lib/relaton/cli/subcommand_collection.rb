require "relaton/cli/full_text_search"

module Relaton
  module Cli
    class SubcommandCollection < Thor
      include Relaton::Cli
      class_option :verbose, aliases: :v, type: :boolean, desc: "Output warnings"

      desc "create COLLECTION", "Create collection"
      option :dir, aliases: :d, desc: "Directory to store collection. " \
                                      "Default is $HOME/.relaton/collections."
      option :author, desc: "Author"
      option :title, desc: "Title"
      option :doctype, desc: "Documents type"

      def create(file)
        dir = directory
        file_path = File.join dir, file
        col = Relaton::Bibcollection.new options
        if File.exist? file_path
          warn "Collection #{file} aready exist"
        else
          FileUtils.mkdir_p dir # unless Dir.exist? dir
          File.write file_path, col.to_yaml, encoding: "UTF-8"
        end
      end

      desc "info COLLECTION", "View collection information"
      option :dir, aliases: :d, desc: "Directory to store collection. " \
                                      "Default is $HOME/.relaton/collections."

      def info(file) # rubocop:disable Metrics/AbcSize
        path = File.join directory, file
        puts "Collection: #{File.basename path}"
        puts "Last updated: #{File.mtime path}"
        puts "File size: #{File.size path}"
        col = Relaton::Bibcollection.new YAML.load_file(path)["root"]
        puts "Number of items: #{col.items.size}"
        puts "Author: #{col.author}"
        puts "Title: #{col.title}"
      end

      desc "list", "List collections"
      option :dir, aliases: :d, desc: "Directory with collections. Default " \
                                      "is $HOME/.relaton/collections."
      option :entries, aliases: :e, type: :boolean, desc: "Show entries"

      def list
        Dir[File.join(directory, "*")].each do |f|
          yml = read_yaml f
          if yml && yml["root"]
            puts File.basename f
            puts_entries yml
          end
        end
      end

      map ls: :list

      desc "get CODE", "Fetch document from collection by ID"
      option :collection, aliases: :c, desc: "Collection to fetch document. " \
        "By default fetch the first match across all collections."
      option :dir, aliases: :d, desc: "Directory with collections. Default " \
                                      "is $HOME/.relaton/collections."
      option :format, aliases: :f, desc: "Output format (xml, abb). " \
        "If not defined the output in a human-readable form."
      option :output, aliases: :o, desc: "Output to the specified file. The " \
        " file's extension (abb, xml) defines output format."

      def get(docid)
        collections.each do |col|
          col[:collection].items.each do |item|
            if item.docidentifier == docid
              output_item(item)
              return
            end
          end
        end
      end

      desc "find TEXT", "Full-text search"
      option :collection, aliases: :c, desc: "Collection to search text. " \
        "By default search across all collections."
      option :dir, aliases: :d, desc: "Directory with collections. Default is " \
                                      "$HOME/.relaton/collections."

      def find(text)
        collections.each do |col|
          searcher = Relaton::FullTextSeatch.new(col[:collection])
          searcher.search text
          if searcher.any?
            puts "Collection: #{File.basename(col[:file])}"
            searcher.print_results
          end
        end
      end

      map search: :find

      desc "fetch CODE", "Fetch a document and store it into a collection"
      option :type, aliases: :t, required: true,
                    desc: "Type of standard to get bibliographic entry for"
      option :year, aliases: :y, type: :numeric,
                    desc: "Year the standard was published"
      option :collection, aliases: :c, required: true,
                          desc: "Collection to store a document"
      option :dir, aliases: :d, desc: "Directory with collections. Default is " \
                                      "$HOME/.relaton/collections."

      def fetch(code) # rubocop:disable Metrics/AbcSize
        doc = Relaton.db.fetch(code, options[:year]&.to_s)
        if doc
          colfile = File.join directory, options[:collection]
          coll = read_collection colfile
          coll << doc
          File.write colfile, coll.to_yaml, encoding: "UTF-8"
        else warn "No matching bibliographic entry found"
        end
      end

      desc "import FILE", "Import document or collection from an XML file " \
                          "into another collection"
      option :collection, aliases: :c, required: true,
                          desc: "Collection to store a document. If " \
                                "collection doesn't exist then it'll be created."
      option :dir, aliases: :d, desc: "Directory with collections. Default is " \
                                      "$HOME/.relaton/collections."

      def import(file) # rubocop:disable Metrics/AbcSize,Metrics/MethodLength
        collfile = File.join directory, options[:collection]
        coll = read_collection collfile
        xml = Nokogiri::XML File.read(file, encoding: "UTF-8")
        if xml.at "relaton-collection"
          if coll
            coll << Relaton::Bibcollection.from_xml(xml)
          else
            coll = Relaton::Bibcollection.from_xml(xml)
          end
        else
          coll ||= Relaton::Bibcollection.new({})
          coll << Relaton::Bibdata.from_xml(xml)
        end
        File.write collfile, coll.to_yaml, encoding: "UTF-8"
      end

      desc "export COLLECTION", "Export collection into XML file"
      option :dir, aliases: :d, desc: "Directory with collections. Default is " \
                                      "$HOME/.relaton/collections."

      def export(file)
        coll = read_collection File.join(directory, file)
        outfile = "#{file.sub(/\.\w+$/, '')}.xml"
        File.write outfile, coll.to_xml(bibdata: true), encoding: "UTF-8"
      end

      private

      # @return [String]
      def directory
        options.fetch :dir, File.join(Dir.home, ".relaton/collections")
      end

      # @param file [String]
      # @return [Hash]
      def read_yaml(file)
        YAML.load_file file if File.file? file
      rescue Psych::SyntaxError
        warn "[relaton-cli] WARNING: the file #{file} isn't a collection."
      end

      # @param file [String]
      # @return [Relaton::Bibcollection, nil]
      def read_collection(file)
        return unless File.file?(file)

        Relaton::Bibcollection.new YAML.load_file(file)["root"]
      end

      # @return [Array<Hash>]
      def collections
        file = options.fetch :collection, "*"
        Dir[File.join directory, file].reduce([]) do |m, f|
          yml = read_yaml f
          if yml && yml["root"]
            m << { collection: Relaton::Bibcollection.new(yml["root"]),
                   file: f }
          end
          m
        end
      end

      # Puts document IDs for each item in tthe cokllection
      # @param hash [Hash] Relaton collection
      def puts_entries(hash)
        return unless options[:entries]

        Relaton::Bibcollection.new(hash["root"]).items.each do |b|
          puts "  #{b.docidentifier}"
        end
      end

      # @param item [Relaton::Bibdata]
      def output_item(item)
        case options[:format]
        when "xml" then puts item.to_xml bibdata: true
        when "abb" then puts item.to_asciibib
        else puts_human_readable_item item
        end
        out = case options[:output]
              when /\.abb$/ then item.to_asciibib
              when /\.xml$/ then item.to_xml bibitem: true
              end
        File.write options[:output], out, encoding: "UTF-8" if out
      end

      # @param item [Relaton::Bibdata]
      def puts_human_readable_item(item) # rubocop:disable Metrics/AbcSize
        puts "Document identifier: #{item.docidentifier}"
        puts "Title: #{item.title.first.title.content}"
        puts "Status: #{item.status.stage}"
        item.date.each { |d| puts "Date #{d.type}: #{d.on || d.from}" }
      end
    end
  end
end
