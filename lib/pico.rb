require 'enumerator'

module Pico
  class PicoTable
    include Enumerable
    def clear; end
    def write(key, value); end
    def read(key); end
    def read_all(key); end
    def delete(key); end
    def delete_all(key); end
    def purge(key); end
    def each; end
    def first; end
    def next_key(key); end
    def nearest_key(key); end
    def []=(k, v); end
    def [](k); read(k); end
    def close; end
    def import(pico); pico.each {|k, v| write(k, v)}; end
    def include?(key); end
    def size; end
  end
end

require 'pico/memory'
require 'pico/bdb'

