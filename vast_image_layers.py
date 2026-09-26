"""Compressed layer sizes from pinned public worker manifests (2026-09-22).

Keys are Docker's unique 12-character layer digest prefixes. Update this catalog
alongside IMAGE when publishing a new worker; unknown images use layer counts.
No registry credentials or runtime network requests are needed.
"""

SERVICE = 'VastRunner'

IMAGE_LAYERS = {
    'ghcr.io/msei99/pbgui-pb8-worker@sha256:bc330893bef1864065dc21835faac725e551383c3455ad4c5908a6713417268e': {
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
        '83ed66c230cd': 5956, '5399c1131ae8': 5559565,
        '0cd8d4a6c566': 1512474, '1a95d3631e9c': 3316962,
        '5449075da220': 6020, '85a2d5403f13': 5953,
        '70716771b76e': 2523, '9e0e66593892': 2946,
        '4084f970b7b9': 3906, 'bee8fbb65e31': 6581,
        '6200ee810d11': 35696, '54b616fa4bf1': 48031,
    },
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

IMAGE_LAYERS['ghcr.io/msei99/pbgui-pb8-worker@sha256:f078b47466f53e3039b13e41905531d9504ca6caca7b57469499fae20b77ec0e'] = {
    **IMAGE_LAYERS['ghcr.io/msei99/pbgui-pb8-worker@sha256:bc330893bef1864065dc21835faac725e551383c3455ad4c5908a6713417268e'],
    'da54abc67442': 8449,
}

IMAGE_LAYERS['ghcr.io/msei99/pbgui-pb8-worker@sha256:8a85444f341a447564fc23e955b34f8a68b1970afc54026334aefa204c6cbc56'] = {
    **IMAGE_LAYERS['ghcr.io/msei99/pbgui-pb8-worker@sha256:bc330893bef1864065dc21835faac725e551383c3455ad4c5908a6713417268e'],
    '3180f399c4af': 9202,
}
IMAGE_LAYERS['ghcr.io/msei99/pbgui-pb8-worker@sha256:bee2e513d77e2c22f5b0671392063c51bb04bd547c7334e614fe918ada2d49e2'] = {
    **IMAGE_LAYERS['ghcr.io/msei99/pbgui-pb8-worker@sha256:bc330893bef1864065dc21835faac725e551383c3455ad4c5908a6713417268e'],
    'b02118494343': 9380,
}
IMAGE_LAYERS['ghcr.io/msei99/pbgui-pb8-worker@sha256:70366b9989a12528245d4c263e0e3dc4350271427afd76f9568dcf29cf33878f'] = {
    **IMAGE_LAYERS['ghcr.io/msei99/pbgui-pb8-worker@sha256:bc330893bef1864065dc21835faac725e551383c3455ad4c5908a6713417268e'],
    '26842a45b418': 10694,
}

IMAGE_LAYERS['ghcr.io/msei99/pbgui-pb8-worker@sha256:8ad62f43decae47fe670f3ac15ba7e4d7c648f0bb030fa511cd4771321ff4327'] = {
    **IMAGE_LAYERS['ghcr.io/msei99/pbgui-pb8-worker@sha256:bc330893bef1864065dc21835faac725e551383c3455ad4c5908a6713417268e'],
    'b7578f7cce41': 11143,
}

IMAGE_LAYERS['ghcr.io/msei99/pbgui-pb8-worker@sha256:09cb0f9ba004db44f3ca02a7b3b03ea3211fd9e6c3c9ea33794cd4c6d24dee30'] = {
    'b4e4c0127190': 49584944, '08457856946d': 24056247,
    '8cab6ce149c2': 64413065, '01a6a9ffe665': 211662335,
    '9fc491314404': 6162299, 'cbbcd353f76c': 25718575,
    '02a27392fe49': 250, 'ef0fda998541': 410659161,
    '1c6431f5c127': 541317199, '14c02e176a01': 201811427,
    'ddb62509502f': 1001827, 'c45da8b4cb34': 248,
    '89aa514fec03': 820822223, 'f270c5494f79': 375407724,
    'b40d97618a75': 366946739, 'a5690081fe59': 206185833,
    '9612ba35fbad': 61014737, 'bdea77f1a062': 170720669,
    '2315f54fcde8': 90246303, '9d201e069dec': 825231,
    '8043bc9f3416': 10459308, '1d495dd0c516': 141363608,
    '5b4a86fa384b': 1307, 'e9da40aec3d0': 2599,
    'a7e7593d61d': 94, '6a77165a8ae9': 11961,
}
