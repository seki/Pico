require 'pico/tc_bdb'

module Pico
  class InBDB < PicoTable
    def self.read(*srcs)
      picos = srcs.collect {|name| self.new(name, 'r')}
      yield(*picos)
    ensure
      picos.each {|pico| pico.close}
    end
    
    def self.create(name)
      pico = self.new(name)
      pico.clear
      yield(pico)
    ensure
      pico.close
    end

    include MonitorMixin
    def initialize(name, mode='w')
      super()
      @bdb = TC_BDB.new
      @bdb.open(name, open_mode(mode))
    end

    def close
      synchronize do
        @bdb.close if @bdb
        @bdb = nil
      end
    end
    
    def write(key, value)
      synchronize do
        @bdb.putdup(key, value)
      end
    end

    def read(key)
      @bdb.get(key)
    end

    def read_all(key)
      synchronize do
        if block_given?
          cursor = @bdb.cursor
          cursor.jump(key)
          while cursor.key
            yield(cursor.val)
            cursor.next
            break unless cursor.key == key
          end
        else
          @bdb.getlist(key)
        end
      end
    end
    
    def delete(key)
      synchronize do
        @bdb.tranbegin
        @bdb.out(key)
        @bdb.trancommit
      end
    end

    def delete_all(key)
      synchronize do
        @bdb.tranbegin
        @bdb.outlist(key)
        @bdb.trancommit
      end
    end

    def each(&blk)
      cursor = @bdb.cursor
      cursor.first
      while cursor.key
        yield(cursor.key, cursor.val)
        cursor.next
      end
      nil
    end

    def first
      cursor = @bdb.cursor
      cursor.first
      cursor.key
    end

    def next_key(key)
      cursor = @bdb.cursor
      cursor.jump(key)
      while cursor.key
        return cursor.key unless cursor.key == key
        cursor.next
      end
    end

    def nearest_key(key)
      cursor = @bdb.cursor
      cursor.jump(key)
      return cursor.key
    end

    def purge(key)
      synchronize do
        @bdb.tranbegin
        n = @bdb.vnum(key) - 1
        n.times { @bdb.out(key) }
        @bdb.trancommit
      end
    end

    def clear
      synchronize do
        @bdb.vanish
      end
    end

    def open_mode(mode)
      if mode == 'r'
        TokyoCabinet::BDB::OREADER
      else
        TokyoCabinet::BDB::OWRITER | TokyoCabinet::BDB::OCREAT
      end
    end

    def import(pico)
      synchronize do
        pico.each do |k, v|
          @bdb.putdup(k, v)
        end
      end
    end

    def size
      @bdb.rnum
    end
  end
end

