require 'helper'

describe Delayed::Worker do
  describe 'backend=' do
    before do
      @clazz = Class.new
      Delayed::Worker.backend = @clazz
    end

    after do
      Delayed::Worker.backend = :test
    end

    it 'sets the Delayed::Job constant to the backend' do
      expect(Delayed::Job).to eq(@clazz)
    end

    it 'sets backend with a symbol' do
      Delayed::Worker.backend = :test
      expect(Delayed::Worker.backend).to eq(Delayed::Backend::Test::Job)
    end
  end

  describe 'job_say' do
    before do
      @worker = Delayed::Worker.new
      @job = double('job', :id => 123, :name => 'ExampleJob', :queue => nil)
    end

    it 'logs with job name and id' do
      expect(@job).to receive(:queue)
      expect(@worker).to receive(:say).
        with('Job ExampleJob (id=123) message', Delayed::Worker.default_log_level)
      @worker.job_say(@job, 'message')
    end

    it 'logs with job name, queue and id' do
      expect(@job).to receive(:queue).and_return('test')
      expect(@worker).to receive(:say).
        with('Job ExampleJob (id=123) (queue=test) message', Delayed::Worker.default_log_level)
      @worker.job_say(@job, 'message')
    end

    it 'has a configurable default log level' do
      Delayed::Worker.default_log_level = 'error'

      expect(@worker).to receive(:say).
        with('Job ExampleJob (id=123) message', 'error')
      @worker.job_say(@job, 'message')
    end
  end

  context 'worker read-ahead' do
    before do
      @read_ahead = Delayed::Worker.read_ahead
    end

    after do
      Delayed::Worker.read_ahead = @read_ahead
    end

    it 'reads five jobs' do
      expect(Delayed::Job).to receive(:find_available).with(anything, 5, anything).and_return([])
      Delayed::Job.reserve(Delayed::Worker.new)
    end

    it 'reads a configurable number of jobs' do
      Delayed::Worker.read_ahead = 15
      expect(Delayed::Job).to receive(:find_available).with(anything, Delayed::Worker.read_ahead, anything).and_return([])
      Delayed::Job.reserve(Delayed::Worker.new)
    end
  end

  context 'worker exit on complete' do
    before do
      Delayed::Worker.exit_on_complete = true
    end

    after do
      Delayed::Worker.exit_on_complete = false
    end

    it 'exits the loop when no jobs are available' do
      worker = Delayed::Worker.new
      Timeout.timeout(2) do
        worker.start
      end
    end
  end

  context 'worker job reservation' do
    before do
      Delayed::Worker.exit_on_complete = true
    end

    after do
      Delayed::Worker.exit_on_complete = false
    end

    it 'handles error during job reservation' do
      expect(Delayed::Job).to receive(:reserve).and_raise(Exception)
      Delayed::Worker.new.work_off
    end

    it 'gives up after 10 backend failures' do
      expect(Delayed::Job).to receive(:reserve).exactly(10).times.and_raise(Exception)
      worker = Delayed::Worker.new
      9.times { worker.work_off }
      expect(lambda { worker.work_off }).to raise_exception Delayed::FatalBackendError
    end

    it 'allows the backend to attempt recovery from reservation errors' do
      expect(Delayed::Job).to receive(:reserve).and_raise(Exception)
      expect(Delayed::Job).to receive(:recover_from).with(instance_of(Exception))
      Delayed::Worker.new.work_off
    end
  end

  context '#say' do
    before(:each) do
      @worker = Delayed::Worker.new
      @worker.name = 'ExampleJob'
      @worker.logger = double('job')
      time = Time.now
      allow(Time).to receive(:now).and_return(time)
      @text = 'Job executed'
      @worker_name = '[Worker(ExampleJob)]'
      @expected_time = time.strftime('%FT%T%z')
    end

    after(:each) do
      @worker.logger = nil
    end

    shared_examples_for 'a worker which logs on the correct severity' do |severity|
      it "logs a message on the #{severity[:level].upcase} level given a string" do
        expect(@worker.logger).to receive(:send).
          with(severity[:level], "#{@expected_time}: #{@worker_name} #{@text}")
        @worker.say(@text, severity[:level])
      end

      it "logs a message on the #{severity[:level].upcase} level given a fixnum" do
        expect(@worker.logger).to receive(:send).
          with(severity[:level], "#{@expected_time}: #{@worker_name} #{@text}")
        @worker.say(@text, severity[:index])
      end
    end

    severities = [{:index => 0, :level => 'debug'},
                  {:index => 1, :level => 'info'},
                  {:index => 2, :level => 'warn'},
                  {:index => 3, :level => 'error'},
                  {:index => 4, :level => 'fatal'},
                  {:index => 5, :level => 'unknown'}]
    severities.each do |severity|
      it_behaves_like 'a worker which logs on the correct severity', severity
    end

    it 'logs a message on the default log\'s level' do
      expect(@worker.logger).to receive(:send).
        with('info', "#{@expected_time}: #{@worker_name} #{@text}")
      @worker.say(@text, Delayed::Worker.default_log_level)
    end
  end

  describe 'plugin registration' do
    it 'does not double-register plugins on worker instantiation' do
      performances = 0
      plugin = Class.new(Delayed::Plugin) do
        callbacks do |lifecycle|
          lifecycle.before(:enqueue) { performances += 1 }
        end
      end
      Delayed::Worker.plugins << plugin

      Delayed::Worker.new
      Delayed::Worker.new
      Delayed::Worker.lifecycle.run_callbacks(:enqueue, nil) {}

      expect(performances).to eq(1)
    end
  end

  describe 'signal handling' do
    SIGNALS = %w[TERM INT]
    SIGNAL_HANDLERS = {}

    before(:each) do
      old_signal_handlers = SIGNALS.each do |signal|
        SIGNAL_HANDLERS[signal] = Signal.trap signal, 'SYSTEM_DEFAULT'
      end
    end

    after(:each) do
      SIGNAL_HANDLERS.each do |signal, signal_handler|
        Signal.trap signal, signal_handler
      end
    end

    class Semaphore
      def initialize(count = 0)
        @count = count
        @mutex = Mutex.new
        @condition = ConditionVariable.new
      end

      def acquire()
        @mutex.synchronize do
          until @count > 0
            @condition.wait(@mutex)
          end
          @count -= 1
        end
      end

      def release(count = 1)
        @mutex.synchronize do
          @count += count
          @condition.broadcast()
        end
      end
    end

    SemaphoreJob = Struct.new(:semaphore) do
      def perform
        semaphore.acquire
      end
    end

    context 'graceful_exit = false' do
      worker_options = {}

      it 'exits after the current job is complete' do
        semaphore = Semaphore.new

        # Enqueue 10 delayed jobs
        expect do
          10.times { Delayed::Job.enqueue SemaphoreJob.new(semaphore) }
        end.to change { Delayed::Job.count }.by(10)
        
        # Start a new worker in a background thread
        worker = Delayed::Worker.new(worker_options)
        thread = Thread.new { worker.start }

        # Complete 3 jobs on the queue
        semaphore.release(3)

        # Send a TERM signal to current process- current job should complete
        # and then worker should exit
        Process.kill 'TERM', 0

        # Allow the current job to complete
        semaphore.release()
        
        thread.join
        expect(Delayed::Job.count).to eq(6)
      end
    end

    context 'graceful_exit = true' do
      worker_options = {graceful_exit: true}

      it 'exits when the queue is drained' do
        semaphore = Semaphore.new

        # Enqueue 10 delayed jobs
        expect do
          10.times { Delayed::Job.enqueue SemaphoreJob.new(semaphore) }
        end.to change { Delayed::Job.count }.by(10)
        
        # Start a new worker in a background thread
        worker = Delayed::Worker.new(worker_options)
        thread = Thread.new { worker.start }

        # Complete 3 jobs on the queue
        semaphore.release(3)

        # Send a TERM signal to current process- all jobs should complete
        Process.kill 'TERM', 0

        # Allow the next 7 jobs to complete
        semaphore.release(7)
        
        thread.join
        expect(Delayed::Job.count).to eq(0)
      end

      it 'exits after the current job if multiple signals are received' do
        semaphore = Semaphore.new

        # Enqueue 10 delayed jobs
        expect do
          10.times { Delayed::Job.enqueue SemaphoreJob.new(semaphore) }
        end.to change { Delayed::Job.count }.by(10)
        
        # Start a new worker in a background thread
        worker = Delayed::Worker.new(worker_options)
        thread = Thread.new { worker.start }

        # Complete 3 jobs on the queue
        semaphore.release(3)

        # Send *two* TERM signals to current process- current job should
        # complete, then worker should exit
        2.times { Process.kill 'TERM', 0 }

        # Allow the next job to complete
        semaphore.release()
        
        thread.join
        expect(Delayed::Job.count).to eq(6)
      end
    end
  end
end
