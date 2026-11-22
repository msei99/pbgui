"""Read native result envelopes without importing API or numerical libraries."""


class InvalidPB9ResultRecord(ValueError):
    """Reject an unsupported result envelope without guessing another generation."""


def result_config_view(document):
    """Borrow a flat config or PB9 result-v1 config; leave artifact bytes unchanged.

    This structural view supports offline browsing. Native validation still owns
    every editable draft and execution handoff.
    """
    if not isinstance(document, dict):
        raise InvalidPB9ResultRecord('Expected a result config object')
    if 'result_version' not in document:
        return document
    from passivbot_runtime import generation_from_schema
    config = document.get('config')
    if (type(document['result_version']) is not int or document['result_version'] != 1
            or set(document) - {'result_version', 'config', 'provenance', 'metrics', 'suite_metrics'}
            or not isinstance(config, dict) or not isinstance(document.get('provenance'), dict)
            or not isinstance(config.get('config_version'), str)
            or generation_from_schema(config['config_version']) != 'pb9'):
        raise InvalidPB9ResultRecord('Invalid or unsupported PB9 result envelope')
    return config
