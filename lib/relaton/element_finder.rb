module Relaton
  module ElementFinder
    attr_reader :document

    def find_text(xpath, element = nil)
      find(xpath, element)&.text
    end

    def find(xpath, element = nil)
      (element || document).at(apply_namespace(xpath))
    end

    def find_xpath(xpath, element = nil)
      element&.xpath(apply_namespace(xpath))
    end

    def apply_namespace(xpath)
      xpath
        .gsub(%r{/([a-zA-Z])}, "/xmlns:\\1")
        .gsub(%r{::([a-zA-Z])}, "::xmlns:\\1")
        .gsub(%r{\[([a-zA-Z][a-z0-9A-Z@/]* ?=)}, "[xmlns:\\1")
        .gsub(%r{\[([a-zA-Z][a-z0-9A-Z@/]*\])}, "[xmlns:\\1")
    end
  end
end
