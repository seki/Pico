require 'rbtree'
require 'rinda/tuplespace'
require 'rinda_eval'

class MultiRBTree
  def split(size, place=nil)
    @_undumped = Thread.current # FIXME
    key = Time.now.to_f
    pid = Process.pid
    place = Rinda::TupleSpace.new unless place
    prev = DRbObject.new(self)
    Rinda::rinda_eval(place) do |ts|
      size.times do
        self.shift
      end
      _,_,_, node = ts.take([:upper_node, key, pid, nil])
      @left = node
      ts.write([:lower_node, key, pid, self])
      ts.read([:shutdown]) rescue exit
      [:done]
    end
    (self.size - size).times do
      self.pop
    end
    place.write([:upper_node, key, pid, self])
    _,_,_, node = place.take([:lower_node, key, pid, nil])
    if @right
      @right.left = node
    end
    @right = node
  end

  attr_accessor :left, :right

  def which_region(key)
    if upper_bound(key)
      if lower_bound(key)
        :within
      else
        :lower
      end
    else
      :upper
    end
  end

  def fwd_while(cur, key, fwd, region)
    while cur
      return false, cur if cur.which_region(key) == region
      node = cur.send(fwd)
      return true, cur unless node
      cur = node
    end
    return false, cur
  end
  
  def browse_node(key, fwd, bwd, region)
    f ,= first
    l ,= last
    return self if f && f < key && l && l > key

    cur = self
    found, cur = fwd_while(cur, key, fwd, region)
    return cur if found
    found, cur = fwd_while(cur, key, bwd, :within)
    cur
  end

  def upper_node(key)
    browse_node(key, :left, :right, :lower)
  end

  def lower_node(key)
    browse_node(key, :right, :left, :upper)
  end
end

if __FILE__ == $0
  ts = Rinda::TupleSpace.new
  DRb.start_service
  rbt = MultiRBTree.new
  5000.times do |n|
    rbt[(n % 49).to_s] = n
  end
  ary = [rbt]
  ary << ary.last.split(1000, ts)
  ary << ary.last.split(1000, ts)
  ary << ary.last.split(1000, ts)
  ary << ary[1].split(500)
  ary.each do |x|
    p [x.first, x.size, (x.__drburi rescue [:local, DRb.uri]), 
       (x.left.last rescue nil), (x.right.first rescue nil)]
  end

  cur = ary[0]
  while cur
    p [cur.first, cur.last]
    cur = cur.right
  end

  x = ary[0]
  %w(0 1 17 171 21 39 99).each do |w|
    p w
    ary.each do |x|
      x = x.upper_node(w)
      p [x.first, x.size, (x.__drburi rescue [:local, DRb.uri]), 
         (x.left.last rescue nil), (x.right.first rescue nil)] if x
    end
    ary.each do |x|
      x = x.lower_node(w)
      p [x.first, x.size, (x.__drburi rescue [:local, DRb.uri]), 
         (x.left.last rescue nil), (x.right.first rescue nil)] if x
    end
  end

  node = ary[0]
  1000.times do |n|
    key = (n % 20 * 100).to_s
    node = node.lower_node(key)
    node[key] = n
  end

  ary.each do |x|
    p [x.first, x.size, (x.__drburi rescue [:local, DRb.uri]), 
       (x.left.last rescue nil), (x.right.first rescue nil)]
  end
  
  ts.write([:shutdown])
end
