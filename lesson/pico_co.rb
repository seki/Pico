require 'pico'
require 'rinda/tuplespace'
require 'rinda_eval'

def create_index(root)
  pico = Pico::InMemoryWOSynchronize.new
  Dir.glob(File.join(root, '**/*.{c,h,cpp,rb}')) do |path|
    File.read(path).scan(/\w\w+/).uniq.each do |word|
      next if /^\d/ =~ word
      pico.write(word, path)
    end
  end
  pico
end

def setup_tasks(ary, place)
  key = Time.now.to_f
  pid = Process.pid
  ary.each do |root|
    Rinda::rinda_eval(place) do |ts|
      _,_,_, dir = ts.take([:root, key, pid, nil])
      pico = create_index(dir)
      pico.extend(DRbUndumped)
      ts.write([:index, dir, pico])
      ts.read([:shutdown]) rescue exit
      [:done]
    end
    place.write([:root, key, pid, root])
  end
  ary.collect do |root|
    _,_, pico = place.take([:index, root, nil])
    pico
  end
end

class PicoStream
  def initialize(pico, buf=256)
    @queue = SizedQueue.new(buf * 2)
    Thread.new do
      pico.each_slice(buf) do |ary|
        ary.each do |k, v|
          @queue.push([k, v])
        end
      end
      @queue.push([:end, nil])
    end
    @key, @value = @queue.pop
  end
  attr_reader :key
  
  def pop
    return @key, @value
  ensure
    @key, @value = @queue.pop
  end
end

def pico_each(picos)
  ary = picos.collect {|pico| PicoStream.new(pico)}
  ary.delete_if {|pico| pico.key == :end}
  while ary.size > 0
    cur = ary.sort_by {|pico| pico.key}.first
    last = cur.key
    while last == cur.key
      yield(cur.pop)
    end
    ary.delete_if {|pico| pico.key == :end}
  end
  nil
end

DRb.start_service
ts = Rinda::TupleSpace.new
ary = setup_tasks(ARGV, ts)

last = nil
c = 0
pico_each(ary) do |k, v|
  if last != k
    p [last, c] if last
    last = k
    c = 1
  else
    c += 1
  end
end
p [last, c]
