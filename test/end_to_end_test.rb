require 'test/unit'
require 'gearman'
require 'support/system'

class EndToEndTest < Test::Unit::TestCase

  def setup
    @system = Gearman::System.new
    @factory = Gearman::Factory.new(@system)
    @system.start
    @ability = 'test'
    @jobs_waited_for = 0
  end

  def test_implicit_return
    having_added_a_task(@ability, '1') do
      worker = @factory.new_worker(@ability) do |data, job|
        '2'
      end

      worker.work
    end

    assert_worker_did_not_fail
    assert_worker_did_not_error
    assert_worker_returned '2'
  end

  def test_explicit_return
    having_added_a_task(@ability, '1') do
      worker = @factory.new_worker(@ability) do |data, job|
        return '2'
      end

      worker.work
    end

    assert_worker_did_not_fail
    assert_worker_did_not_error
    assert_worker_returned '2'
  end

  def having_added_a_task(*task_args)
    task_set = @factory.new_task_set
    task = @factory.new_task *task_args

    task.on_complete do |data|
      @worker_success = data
    end

    task.on_exception do |exception|
      @worker_error = exception
    end

    task.on_fail do
      @worker_failure = true
    end

    task_set.add_task(task)

    yield
    
    task_set.wait(5)

    @jobs_waited_for += 1
  end

  def assert_worker_did_not_fail
    assert_nil(@worker_failure)
  end

  def assert_worker_did_not_error
    assert_nil(@worker_error)
  end

  def assert_worker_returned(expected)
    assert_equal(expected, @worker_success)
  end

  def assert_we_at_least_tried
    assert_equal(1, @jobs_waited_for)
  end

  def teardown
    @system.stop
    assert_we_at_least_tried
  end

end
