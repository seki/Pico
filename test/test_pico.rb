require 'test/unit'
require 'pico'
require 'pico/picopico'

class PicoTest < Test::Unit::TestCase
  def setup
    @pico = Pico::InMemory.new
  end

  def teardown
  end

  def test_empty
    assert_equal(@pico.first, nil)
    assert_equal(@pico.nearest_key(''), nil)
    assert_equal(@pico.next_key(''), nil)
    assert_equal(@pico[''], nil)
    assert_equal(@pico.size, 0)
  end

  def test_key
    10.times do |n|
      @pico.write(n % 3, n)
    end
    assert_equal(@pico.first, '0')
    assert_equal(@pico.next_key('0'), '1')
    assert_equal(@pico.next_key('2'), nil)
    assert_equal(@pico.size, 10)
  end
  
  def test_dup
    10.times do |n|
      @pico.write(n % 3, n)
    end
    assert_equal(@pico.read_all('0'), ['0', '3', '6', '9'])
    assert_equal(@pico.read('0'), '0')
    assert_equal(@pico.size, 10)
    @pico.purge('1')
    assert_equal(@pico.read_all('1'), ['7'])
    assert_equal(@pico.size, 8)
  end
end

class PicoTestWOSync < PicoTest
  def setup
    @pico = Pico::InMemoryWOSynchronize.new
  end
end

class PicoTestBDB < PicoTest
  def setup
    @pico = Pico::InBDB.new('test.tc')
    @pico.clear
  end

  def teardown
    @pico.close
    File.unlink('test.tc') rescue nil
  end
end

class PicoTestPicoPico < PicoTest
  def setup
    @primary = Pico::InMemory.new
    @pico = Pico::PicoPicoTable.new(@primary)
    @five = Pico::InMemory.new
    @pico.add_pico('5', @five)
    @a = Pico::InMemory.new
    @pico.add_pico('a', @a)
  end
  
  def teardown
    @pico.close
  end
end
