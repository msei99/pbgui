"""Compressed layer sizes from the pinned public worker manifest (2026-09-15).

Keys are Docker's unique 12-character layer digest prefixes. Update this catalog
alongside IMAGE when publishing a new worker; unknown images use layer counts.
No registry credentials or runtime network requests are needed.
"""

SERVICE = 'VastRunner'

IMAGE_LAYERS = {
    'ghcr.io/msei99/pbgui-pb8-worker@sha256:b6f61c54b546640f5f00e386c10a27380e0ed8715788bcc8c4b597eedff58dbc': {
        'b4e4c0127190': 49584944, '08457856946d': 24056247,
        '8cab6ce149c2': 64413065, '01a6a9ffe665': 211662335,
        '9fc491314404': 6162299, 'cbbcd353f76c': 25718575,
        '02a27392fe49': 250, '87520efcd430': 409255582,
        '58dffdf426cc': 541316286, '2a48e8037330': 201811370,
        'ea020b03a317': 250, 'ea381c80ad7f': 820822233,
        'd7fd6a7c5cd9': 375407673, '729b88e2fe7a': 366946736,
        '3cd09cc19990': 206185767, 'ed24936cf12e': 61014752,
        'e0c442140087': 170720643, 'd968bb3afa84': 90246313,
        'a00d5f932217': 825232, 'c1f0a21229ab': 10459334,
        '95f6451ab6d2': 138179815, '988542c77ce2': 1313,
        '8fd57454b139': 2597, '9ed4eee7f53b': 94,
        '726b73bb580e': 5084, 'ea7517ae3a1d': 1001828,
        '547de3362ec5': 5724,
    },
}
