require "thor"
require "thor/hollaback"
require "relaton"
require_relative "cli/command"

module Relaton
  def self.db(dir = nil)
    Cli.relaton dir
  end

  module Cli
    class RelatonDb
      include Singleton

      DBCONF = "#{Dir.home}/.relaton/dbpath".freeze

      # @param dir [String, nil]
      # @return [Relaton::Db]
      def db(dir)
        if dir
          File.write DBCONF, dir, encoding: "UTF-8"
          @db = Relaton::Db.new dir, nil
        else
          @db ||= Relaton::Db.new dbpath, nil
        end
      end

      private

      # @return [String] path to DB
      def dbpath
        if File.exist?(DBCONF)
          File.read(DBCONF, encoding: "UTF-8")
        else "#{Dir.home}/.relaton/cache"
        end
      end
    end

    class << self
      def start(arguments)
        Relaton::Cli::Command.start(arguments)
      end

      # Relaton
      #
      # Based on current setup, we need to initiate a Db instance to
      # register all of it's supported processor backends. To make it
      # easier we have added it as a class method so we can use this
      # whenever necessary.
      #
      # @param dir [String, nil]
      # @return [Relaton::Db]
      def relaton(dir)
        RelatonDb.instance.db dir
      end

      # @param content [Nokogiri::XML::Document]
      # @return [RelatonBib::BibliographicItem,
      #   RelatonIsoBib::IsoBibliongraphicItem]
      def parse_xml(doc)
        if (proc = Cli.processor(doc))
          proc.from_xml(doc.to_s)
        else
          RelatonBib::XMLParser.from_xml(doc.to_s)
        end
      end

      # @param doc [Nokogiri::XML::Element]
      # @return [RelatonIso::Processor, RelatonIec::Processor,
      #   RelatonNist::Processor, RelatonIetf::Processot,
      #   RelatonItu::Processor, RelatonGb::Processor,
      #   RelatonOgc::Processor, RelatonCalconnect::Processor]
      def processor(doc)
        docid = doc.at "docidentifier"
        proc = get_proc docid
        return proc if proc

        Relaton::Registry.instance.by_type(docid&.text&.match(/^\w+/)&.to_s)
      end

      private

      # @param doc [Nokogiri::XML::Element]
      # @return [RelatonIso::Processor, RelatonIec::Processor,
      #   RelatonNist::Processor, RelatonIetf::Processot,
      #   RelatonItu::Processor, RelatonGb::Processor,
      #   RelatonOgc::Processor, RelatonCalconnect::Processor]
      def get_proc(docid)
        return unless docid && docid[:type]

        Relaton::Registry.instance.by_type(docid[:type])
      end
    end
  end
end
