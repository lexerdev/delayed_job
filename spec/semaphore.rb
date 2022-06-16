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

SemaphoreJob = Struct.new(:start_semaphore, :end_semaphore, :index) do
  def perform
    puts("Starting job #{index}")
    start_semaphore.acquire()
    puts("Running job #{index}")
    end_semaphore.release()
    puts("Finished job #{index}")
  end
end