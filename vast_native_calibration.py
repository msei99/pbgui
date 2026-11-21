"""Bounded PB9 cohort measurements shared by the host and native worker.

Protocol 5 measures whole completed evaluation cohorts, including host overhead.
It does not establish full CUDA dispatch width or a candidate-bar memory limit.
"""
import math
from pathlib import Path
import re

SERVICE = 'VastCalibration'
MEASUREMENT = 'native_cohort_v1'
_COHORT = re.compile(
    r'GPU cohort \| gen=(\d+) phase=generation status=(\w+) \| '
    r'attempts=(\d+) prefilter=(\d+) gpu_rows=(\d+|unknown) wall_s=([0-9.]+) '
    r'attempts/s=\S+ gpu_rows/s=\S+'
)


class NativeCohortReader:
    """Incrementally retain cohort totals without rescanning or truncating evidence."""

    def __init__(self, population=None):
        """Bind one fresh process's requested population and first-use exclusion."""
        self.population = population
        self.offset, self.partial, self.identity = 0, b'', None
        self.cold_seen, self.failed, self.last_generation = False, False, -1
        self.cohorts = self.candidates = self.rows = 0
        self.seconds = 0.0

    def _line(self, line):
        """Count each complete, fully GPU-backed generation exactly once."""
        match = _COHORT.search(line)
        if not match:
            return
        generation, status, attempts, rejected, rows, wall = match.groups()
        if status == 'failed':
            self.failed = True
        if status != 'complete' or int(generation) <= self.last_generation:
            return
        self.last_generation = int(generation)
        if not self.cold_seen:
            self.cold_seen = True
            return
        if rows == 'unknown':
            return
        try:
            count, gpu_rows, seconds = int(attempts), int(rows), float(wall)
        except ValueError:
            return
        if (int(rejected) or count <= 0 or gpu_rows < count
                or (self.population is not None and count != self.population)
                or not math.isfinite(seconds) or seconds <= 0):
            return
        self.cohorts += 1
        self.candidates += count
        self.rows += gpu_rows
        self.seconds += seconds

    def read(self, log_path):
        """Consume bounded chunks; a replacement/truncated file invalidates this case."""
        if not log_path.is_file():
            return None
        stat = log_path.stat()
        identity = (stat.st_dev, stat.st_ino)
        if ((self.identity is not None and identity != self.identity) or stat.st_size < self.offset):
            self.failed = True
        self.identity = identity
        with log_path.open('rb') as stream:
            stream.seek(self.offset)
            chunk = stream.read(8 * 1024 * 1024)
            self.offset = stream.tell()
        lines = (self.partial + chunk).split(b'\n')
        self.partial = lines.pop()
        if len(self.partial) > 65536:
            self.failed = True
            self.partial = b''
        for line in lines:
            self._line(line.decode(errors='replace'))
        if self.failed or not self.cohorts:
            return None
        return {'source': MEASUREMENT, 'completed_cohorts': self.cohorts,
                'warmup_cohorts': 1, 'completed_candidates': self.candidates,
                'gpu_rows': self.rows, 'rate_window_seconds': self.seconds,
                'rate_per_second': self.candidates / self.seconds, 'valid': True}


def native_calibration_progress(log_path: Path, *, reader=None) -> dict | None:
    """Read new bytes through the case-owned accumulator, or one bounded probe."""
    return (reader if reader is not None else NativeCohortReader()).read(log_path)


def validate_native_case(row: dict, population: int, sample_seconds: int) -> None:
    """Require native evidence and recompute its rate before recommending a width."""
    if not isinstance(row, dict):
        raise ValueError('Invalid native cohort evidence')
    integers = ('completed_cohorts', 'completed_candidates', 'gpu_rows')
    seconds, rate = row.get('rate_window_seconds'), row.get('rate_per_second')
    if (row.get('source') != MEASUREMENT or row.get('warmup_cohorts') != 1
            or row.get('population_size') != population
            or row.get('requested_batch_size') != population
            or row.get('valid') is not True
            or any(type(row.get(key)) is not int or row[key] <= 0 for key in integers)
            or row['gpu_rows'] < row['completed_candidates']
            or row['completed_candidates'] != population * row['completed_cohorts']
            or type(seconds) not in (int, float) or not math.isfinite(seconds)
            or seconds < sample_seconds
            or type(rate) not in (int, float) or not math.isfinite(rate) or rate <= 0
            or not math.isclose(rate, row['completed_candidates'] / seconds, rel_tol=1e-9)
            or any(key in row for key in ('all_dispatches_full', 'candidate_bars'))):
        raise ValueError('Invalid native cohort evidence')
