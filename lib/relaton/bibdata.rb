require "date"

module Relaton
  class Bibdata
    URL_TYPES = %i[uri xml pdf doc html rxl txt].freeze

    # @return [RelatonBib::BibliographicItem]
    attr_reader :bibitem

    # @param bibitem [RelatonBib::BibliographicItem]
    def initialize(bibitem)
      @bibitem = bibitem
    end

    def docidentifier
      @bibitem.docidentifier.first&.id
    end

    # def doctype
    #   @bibitem.type
    # end

    # From http://gavinmiller.io/2016/creating-a-secure-sanitization-function/
    FILENAME_BAD_CHARS = ["/", '\\', "?", "%", "*", ":", "|", '"', "<", ">",
                          ".", " "].freeze

    def docidentifier_code
      return "" if docidentifier.nil?

      FILENAME_BAD_CHARS.reduce(docidentifier.downcase) do |result, bad_char|
        result.gsub(bad_char, "-")
      end
    end

    DOC_NUMBER_REGEX = /([\w\/]+)\s+(\d+):?(\d*)/.freeze
    def doc_number
      docidentifier&.match(DOC_NUMBER_REGEX) ? $2.to_i : 999999
    end

    def self.from_xml(source)
      bi = Relaton::Cli.parse_xml(source)
      new(bi) if bi
    end

    def to_xml(opts = {})
      options = { bibdata: true, date_format: :full }.merge(
        opts.select { |k, _v| k.is_a? Symbol },
      )
      @bibitem.to_xml(**options)
    end

    def to_h
      URL_TYPES.reduce(@bibitem.to_hash) do |h, t|
        value = send t
        h[t.to_s] = value
        h
      end
    end

    def to_yaml
      to_h.to_yaml
    end

    # rubocop:disable Metrics/CyclomaticComplexity, Metrics/AbcSize
    # rubocop:disable Metrics/MethodLength, Metrics/PerceivedComplexity, Style/MissingRespondToMissing

    def method_missing(meth, *args)
      if @bibitem.respond_to?(meth)
        @bibitem.send meth, *args
      elsif URL_TYPES.include? meth
        link = @bibitem.link.detect do |l|
          l.type == meth.to_s || (meth == :uri && l.type.nil?)
        end
        link&.content&.to_s
      elsif URL_TYPES.include? meth.match(/^\w+(?==)/).to_s.to_sym
        /^(?<type>\w+)/ =~ meth
        link = @bibitem.link.detect do |l|
          l.type == type || (type == "uri" && l.type.nil?)
        end
        if link
          link.content = args[0]
        else
          @bibitem.link << RelatonBib::TypedUri.new(type: type, content: args[0])
        end
      else
        super
      end
    end
    # rubocop:enable Metrics/CyclomaticComplexity, Metrics/AbcSize
    # rubocop:enable Metrics/MethodLength, Metrics/PerceivedComplexity, Style/MissingRespondToMissing
  end
end
