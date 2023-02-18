module Relaton
  module Cli
    module DataFetcher
      def fetch(source, options)
        processor = Relaton::Registry.instance.find_processor_by_dataset source
        unless processor
          warn "[relaton-cli] WARNING: no processor found for #{source}"
          return
        end

        opts = {}
        opts[:output] = options[:output] if options[:output]
        opts[:format] = options[:format] if options[:format]
        processor.fetch_data source, opts
      end

      extend DataFetcher
    end
  end
end
