from __future__ import annotations

from mqnode.chains.btc.reorg import find_common_ancestor_height


def test_find_common_ancestor_height_walks_back_to_matching_hash():
    local_hashes = {
        5: 'local-5',
        4: 'local-4',
        3: 'shared-3',
        2: 'shared-2',
    }
    canonical_hashes = {
        5: 'canonical-5',
        4: 'canonical-4',
        3: 'shared-3',
        2: 'shared-2',
    }

    common_height = find_common_ancestor_height(
        5,
        lambda height: local_hashes.get(height),
        lambda height: canonical_hashes[height],
    )

    assert common_height == 3
