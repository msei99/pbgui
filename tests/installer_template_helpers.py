"""Read shared installer task labels and scalar selections in their PB8 defaults."""
from pathlib import Path


def pb8_playbook_source(path):
    """Expand only product/label scalars; retain all commands, conditions and ordering."""
    source = Path(path).read_text(encoding='utf-8')
    for old, new in [("{{ passivbot_label | default('PB8') }}", 'PB8'),
                     ('{{ passivbot_label }}', 'PB8'), ('{{ passivbot_major }}', '8'),
                     ('{{ passivbot_product }}', 'pb8')]:
        source = source.replace(old, new)
    return source
