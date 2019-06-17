"""
    metrics_collector.py
"""
import logging
import datetime
import json
from frivenmeld.loggingsetup import APP_LOGNAME

class MetricsCollector():
    """
        Accumulates measurements and samples
        of the app
    """

    def __init__(self):
        self._start_time = datetime.datetime.now()
        self._end_time = None
        self._num_matches = 0
        self._max_samples = 10
        self._num_samples = 0
        self._sample_rows = []
        self._logger = logging.getLogger(APP_LOGNAME)

    def increment_matches(self):
        """
            Call this to record that a match was found
        """
        self._num_matches += 1

    def mark_end_time(self):
        """
            Call this when the script
            is considered finished
        """
        self._end_time = datetime.datetime.now()

    def add_sample_row(self, row_dict):
        """
            Accumulate samples
            silently ignores after max_samples
        """
        if self._num_samples < self._max_samples:
            self._sample_rows.append(row_dict)
            self._num_samples += 1

    def _get_duration(self):
        """
            returns the duration of ths script
            in minutes and seconds
        """

        assert self._end_time

        self._logger.debug("Calculating delta between %s and %s",
                           self._end_time,
                           self._start_time)
        delta = self._end_time - self._start_time
        total_seconds = int(delta.total_seconds())
        self._logger.debug("Total seconds is %s", total_seconds)

        minutes = int(total_seconds / 60)
        seconds = int(total_seconds % 60)

        self._logger.debug("Duration is %s min %s seconds",
                           minutes,
                           seconds)

        return minutes, seconds

    def _get_sample_output_as_json(self):
        """
            Converts sample rows into a json string
        """
        as_json = json.dumps(self._sample_rows, sort_keys=True, indent=4)
        return as_json

    def print_summary(self):
        """
            Writes a summary according to these project requirements:
            Elapsed Time: X minutes, X seconds <- Time it took to
                                                  run the entire script
            Total Matches: X <- Total number of rows matched
                                between the Doximity dataset
                                and the vendor dataset
            Sample Output: <10 JSON formatted rows> <- Example of the rows
                                                       that would be loaded
                                                       to the warehouse

            Note: "SQL DDL" output is not included here
        """

        minutes, seconds = self._get_duration()
        print("Elapsed Time: {} minutes, {} seconds".format(minutes, seconds))
        print("Total Matches: {}".format(self._num_matches))

        json_samples = self._get_sample_output_as_json()
        print("Sample Output: {}".format(json_samples))

# pylint: disable=invalid-name
if __name__ == "__main__":

    from frivenmeld.loggingsetup import init_logging                         # pragma: no cover
    import time                                                              # pragma: no cover
    init_logging(logging.DEBUG)                                              # pragma: no cover

    mcollector = MetricsCollector()                                          # pragma: no cover
    time.sleep(2 * 60 + 15)                                                  # pragma: no cover
    mcollector.add_sample_row(row_dict={'cat': 'meow', 'color': 'red'})      # pragma: no cover
    mcollector.add_sample_row(row_dict={'cat': 'shivvers', 'color': 'red'})  # pragma: no cover
    mcollector.add_sample_row(row_dict={'cat': 'tree', 'color': 'black'})    # pragma: no cover
    mcollector.mark_end_time()                                               # pragma: no cover
    mcollector.print_summary()                                               # pragma: no cover

# end
