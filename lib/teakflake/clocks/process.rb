# frozen_string_literal: true

module Teakflake
  class ProcessClock
    def millis
      Process.clock_gettime(Process::CLOCK_REALTIME, :millisecond)
    end
  end
end
