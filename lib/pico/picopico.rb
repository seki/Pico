require 'pico'
require 'rbtree'

module Pico
  class PicoPicoTable < PicoTable
    include MonitorMixin
    def initialize(primary)
      super()
      @primary = primary
      @picos = RBTree.new
      @picos[''] = @primary
    end

    def self.pico_delegate(*args)
      args.each do |s|
        module_eval("def #{s}(*a); found_pico(a[0].to_s) {|pc| pc.#{s}(*a)}; end")
      end
    end

    def found_pico(key, &blk)
      synchronize do
        pair = @picos.upper_bound(key)
        pico = pair ? pair[1] : @primary
        return yield(pico)
      end
    end
    
    def add_pico(fence, pico)
      synchronize do
        found_pico(fence) do |old|
          key = old.nearest_key(fence)
          while key
            old.read_all(key) do |v|
              pico.write(key, v)
            end
            old.delete_all(key)
            key = old.next_key(key)
          end
        end
        @picos[fence] = pico
      end
    end

    def each_pico(&blk)
      @picos.each(&blk)
    end

    pico_delegate(:write, :read, :read_all, :delete, :delete_all, :purge)
    pico_delegate('[]', '[]=')
    pico_delegate(:nearest_key, :next_key, :include?)

    def first
      @picos.first[1].first
    end

    def each(&blk)
      each_pico do |fence, pico|
        pico.each(&blk)
      end
    end

    def clear
      each_pico do |fence, pico|
        pico.clear
      end
    end

    def close
      each_pico do |fence, pico|
        pico.close
      end
      @primary = nil
      @picos = nil
    end

    def size
      sz = 0
      each_pico do |fence, pico|
        sz += pico.size
      end
      sz
    end
  end
end
