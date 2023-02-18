# require "forwardable"

module Relaton
  class FullTextSeatch
    # extend Forwardable

    # def_delegators :@collections, :<<

    # @return Regexp
    attr_reader :regex

    # @param collection [Relaton::Bibcollection]
    def initialize(collection)
      @collection = collection
    end

    # @param text [String]
    # @return [Array<Hash>]
    def search(text)
      @regex = %{(.*?)(.{0,20})(#{text})(.{0,20})(.*)}
      @matches = @collection.items.reduce({}) do |m, item|
        # m + results(col, rgx)
        res = result item
        m[item.id] = res if res.any?
        m
      end
    end

    def print_results
      @matches.each do |docid, attrs|
        puts "  Document ID: #{docid}"
        print_attrs attrs, 4
      end
    end

    # @return [Boolean]
    def any?
      @matches.any?
    end

    private

    # rubocop:disable Metrics/AbcSize,Metrics/CyclomaticComplexity,Metrics/MethodLength,Metrics/PerceivedComplexity
    def print_attrs(attrs, indent)
      ind = " " * indent
      if attrs.is_a? String then puts ind + attrs
      elsif attrs.is_a? Hash
        attrs.each do |key, val|
          pref = "#{ind}#{key}:"
          if val.is_a? String then puts pref + " " + val
          else
            puts pref
            print_attrs val, indent + 2
          end
        end
      elsif attrs.is_a? Array then attrs.each { |v| print_attrs v, indent + 2 }
      end
    end

    # @param item [Relaton::Bibdata]
    # @return [Hash]
    def result(item)
      if item.is_a? String
        message $~ if item.match regex
      elsif item.respond_to? :reduce
        item.reduce([]) do |m, i|
          res = result i
          m << res if res && !res.empty?
          m
        end
      else
        item.instance_variables.reduce({}) do |m, var|
          res = result item.instance_variable_get(var)
          m[var.to_s.tr(":@", "")] = res if res && !res.empty?
          m
        end
      end
    end
    # rubocop:enable Metrics/AbcSize,Metrics/CyclomaticComplexity,Metrics/MethodLength,Metrics/PerceivedComplexity

    # @param match [MatchData]
    # @return [String]
    def message(match)
      msg = ""
      msg += "..." unless match[1].empty?
      msg += "#{match[2]}\e[4m#{match[3]}\e[24m#{match[4]}"
      msg += "..." unless match[5].empty?
      msg
    end
  end
end
