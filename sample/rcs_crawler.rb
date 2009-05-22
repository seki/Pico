require 'rcs'
require 'stringio'

class RCS
  def RCS.parse_str(str)
    rcs = RCS.new
    f = StrintIO.new(str)
    Parser.new(f).parse(
      Parser::PhraseVisitor.new(
	Parser::RCSVisitor.new(
	  InitializeVisitor.new(rcs))))
  end
end

class RCSCrawler
  class Emitter
    def initialize(branches)
      @branches = branches
    end

    def delta=(delta)
      @delta = delta
      @trunk = @delta.rev.on_trunk?
    end
    
    def log(&blk)
      branch = @branches[@delta.rev.branch.magicalize]
      rec = [@delta.author, @delta.log, @delta.date, branch]
      yield(@delta.rev, :log, rec)
    end
    
    def add(line, &blk)
      @trunk ? add_trunk(line, &blk) : add_branch(line, &blk)
      line
    end
    
    def del(line, &blk)
      @trunk ? del_trunk(line, &blk) : del_branch(line, &blk)
      line
    end

    def add_trunk(line)
      yield(@delta.prevrev, :del, line)
    end

    def add_branch(line)
      yield(@delta.rev, :add, line)
    end

    def del_trunk(line)
      yield(@delta.prevrev, :add, line)
    end

    def del_branch(line)
      yield(@delta.rev, :del, line)
    end

    def initial_text(rev, text)
      text.lines.each do |line|
        yield(rev, :add, line)
      end
    end
  end

  def crawl(rcs, &blk)
    branches = Hash.new
    rcs.symbols.each do |k, v|
      branches[v] = k if RCS::Revision::Branch === v
    end
    out = Emitter.new(branches)
    text = Hash.new
    text[rcs.head] = RCS::Text.new(rcs[rcs.head].text)
    min = rcs.head
    rcs.each_delta do |d|
      out.delta = d
      out.log(&blk)
      next unless d.prevrev
      text[d.rev] = text[d.prevrev].patch(d.text,
                                          lambda {|line| out.add(line, &blk)},
                                          lambda {|line| out.del(line, &blk)})
      min = d.rev if d.rev < min
      text.delete(d.prevrev) if rcs[d.prevrev].branches.size == 0
    end
    out.initial_text(min, text[min], &blk)
  end
end

if __FILE__ == $0
  rcs = RCS.parse(ARGV.shift)
  RCSCrawler.new.crawl(rcs) do |rev, mode, line|
    p [rev.to_s, mode, line]
  end
end
