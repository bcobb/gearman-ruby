require 'test/unit'
require 'gearman'
require 'support/system'

class EndToEndTest < Test::Unit::TestCase

  def setup
    Gearman::Util.logger = Logger.new(STDOUT).tap { |l| l.level = Logger::DEBUG }
    @system = Gearman::System.new
    @factory = Gearman::Factory.new(@system)
    @system.start
    @ability = 'test'
  end

  def test_something_simple
    got = nil

    worker = @factory.new_worker(@ability) do |data, job|
      got = data
    end

    @factory.run_task @ability, '1'

    worker.work

    assert_not_nil(got)
  end

  def test_explicit_return
    got = nil
    worker = @factory.new_worker(@ability) do |data, job|
      return true
    end

    task_set = @factory.new_task_set
    task = @factory.new_task @ability, '1'
    task.on_complete do |data|
      got = data
    end

    task_set.add_task(task)

    worker.work

    task_set.wait(5)

    assert_not_nil(got)
  end

  def teardown
    @system.stop
  end

end
