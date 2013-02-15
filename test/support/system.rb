require 'gearman'
require 'timeout'

module Gearman

  class System

    class ReadTimeout < StandardError ; end

    attr_reader :io, :servers

    def initialize
      @io = nil
      @servers = ['localhost:4730']
    end

    def start
      @io = IO.popen "gearmand"

      read
    end

    def read
      begin
        Timeout.timeout(1, ReadTimeout) do
          to_read, _ = IO::select([@io])
          to_read.each(&:read)
        end
      rescue ReadTimeout
        nil
      end
    end

    def stop
      if started?
        Process.kill('KILL', @io.pid)
        @io.close
      end
    end

    def started?
      if @io
        not @io.closed?
      end
    end

  end

  class Factory

    def initialize(system)
      @system = system
    end

    def new_client
      Client.new(@system.servers)
    end

    def new_task_set
      TaskSet.new(new_client)
    end

    def new_task(function, data = nil, options = {})
      Task.new(function, data, options)
    end

    def run_task(function, data = nil, options = {})
      new_task_set.add_task(new_task(function, data, options))
    end

    def new_worker(ability, &block)
      Worker.new(@system.servers).tap do |worker|
        worker.add_ability(ability, &block)
      end
    end

  end

end
