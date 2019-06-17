"""
   test_metrics_collector.py

   unit tests for test_metrics_collector.py
"""
from frivenmeld.metrics_collector import MetricsCollector

def test_public_interface():
    """
        test static init_logging()
    """
    mcollector = MetricsCollector()

    for _ in range(20):
        mcollector.add_sample_row(row_dict={'cat': 'meow', 'color': 'red'})

    for _ in range(50):
        mcollector.increment_matches()

    mcollector.mark_end_time()

    mcollector.print_summary()
