"""Export PB8's GPU metric validator contract using its installed runtime."""

import hashlib
import inspect
import json
from pathlib import Path


def export_contract():
    """Include canonical names and schema aliases only when PB8 accepts them."""
    from config import metrics as schema
    from optimization.gpu import metrics, metric_registry

    names = set(metrics.SUPPORTED_METRICS) | set(schema.CURRENCY_METRICS) | set(schema.SHARED_METRICS) | set(schema.METRIC_ALIASES)
    for name in list(names):
        names.update((name + '_usd', name + '_btc', 'usd_' + name, 'btc_' + name))
    allowed = []
    for name in sorted(names):
        try:
            metrics.validate_gpu_metric_names([name])
        except ValueError:
            continue
        allowed.append(name)
    return {
        'schema_version': 1,
        'supported_metrics': sorted(set(metrics.SUPPORTED_METRICS)),
        'allowed_metrics': allowed,
        'exact_only_metrics': sorted(metric_registry.GPU_EXACT_ONLY_METRICS),
        'source_sha256': {module.__name__: hashlib.sha256(Path(inspect.getfile(module)).read_bytes()).hexdigest()
                          for module in (schema, metrics, metric_registry)},
    }


if __name__ == '__main__':
    print(json.dumps(export_contract(), indent=4))
