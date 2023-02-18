module Relaton
  module Cli
    class SubcommandDb < Thor
      include Relaton::Cli
      class_option :verbose, aliases: :v, type: :boolean, desc: "Output warnings"

      desc "create DIR", "Create new cache DB. Default DIR is "\
        "/home/user/.relaon/cache/"

      def create(dir = nil)
        db = Relaton.db (dir && File.expand_path(dir))
        path = db.instance_variable_get(:@db).dir
        warn "Cache DB is in \"#{path}\""
      end

      desc "mv DIR", "Move cache DB to a new directory"

      def mv(dir)
        new_path = File.expand_path dir
        path = Relaton.db.mv new_path
        if path
          File.write Cli::RelatonDb::DBCONF, path, encoding: "UTF-8"
          warn "Cache DB is moved to \"#{path}\""
        end
      end

      desc "clear", "Clear cache DB"

      def clear
        Relaton.db.clear
        warn "Cache DB is cleared"
      end

      desc "fetch CODE", "Fetch Relaton XML for Standard identifier CODE from "\
        "cache DB"
      option :type, aliases: :t, desc: "Type of standard to "\
        "get bibliographic entry for"
      option :format, aliases: :f, desc: "Output format (xml, yaml, bibtex). "\
        "Default xml."
      option :year, aliases: :y, type: :numeric, desc: "Year the standard was "\
        "published"

      def fetch(code)
        io = IO.new($stdout.fcntl(::Fcntl::F_DUPFD), mode: "w:UTF-8")
        opts = options.merge(fetch_db: true)
        io.puts(fetch_document(code, opts) || supported_type_message)
      end

      desc "fetch_all TEXT", "Query for all documents in a cache DB for a "\
        "certain string"
      option :edition, aliases: :e, desc: "Filter entries by edition"
      option :year, aliases: :y, desc: "Filter entries by year"
      option :format, aliases: :f, desc: "Output format (xml, yaml, bibtex). "\
        "Default xml."

      def fetch_all(text = nil) # rubocop:disable Metrics/AbcSize
        io = IO.new($stdout.fcntl(::Fcntl::F_DUPFD), mode: "w:UTF-8")
        opts = options.each_with_object({}) do |(k, v), o|
          o[k.to_sym] = v unless k == "format"
        end
        Relaton.db.fetch_all(text, **opts).each do |doc|
          io.puts serialize(doc, options[:format])
        end
      end

      desc "doctype REF", "Detect document type from REF"

      def doctype(ref)
        io = IO.new($stdout.fcntl(::Fcntl::F_DUPFD), mode: "w:UTF-8")
        io.puts Relaton.db.docid_type(ref)
      end
    end
  end
end
