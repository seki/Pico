require 'enumerator'
require 'monitor'
require 'rbtree'

module Pico
  class InMemory < PicoTable
    include MonitorMixin
    def initialize
      super()
      @tree = create_tree
    end

    def close
      synchronize do
        @tree = nil
      end
    end

    def write(key, value)
      synchronize do
        @tree[key.to_s] = value.to_s
      end
    end
    
    def read(key)
      @tree[key.to_s]
    end

    def read_all(key)
      synchronize do
        if block_given?
          @tree.bound(key, key) do |k, v|
            yield(v)
          end
          nil
        else
          ary = []
          read_all(key) {|v| ary << v}
          ary
        end
      end
    end
    
    def delete(key)
      synchronize do
        @tree.delete(key)
      end
    end
    
    def delete_all(key)
      synchronize do
        while @tree.delete(key)
          ;
        end
      end
    end
    
    def each(&blk)
      synchronize do
        @tree.each(&blk)
      end
    end
    
    def clear
      synchronize do
        @tree.clear
      end
    end

    def first
      key, value = @tree.first
      key
    end
    
    def next_key(key)
      synchronize do
        return nil if @tree.empty?
        @tree.bound(key, @tree.last[0]) do |k, v|
          return k unless key == k
        end
        nil
      end
    end

    def nearest_key(key)
      key, value = @tree.lower_bound(key)
      return key
    end

    def purge(key)
      synchronize do
        n = -1
        @tree.bound(key, key) { n+= 1 }
        n.times { @tree.delete(key) }
      end
    end

    def size
      @tree.size
    end
    
    def create_tree
      MultiRBTree.new
    end
  end

  class InMemoryWOSynchronize < InMemory
    def synchronize
      yield
    end
  end
end

