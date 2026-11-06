from simrun.config import DEFAULT_BASE_IMAGE, ClusterConfig


def test_base_image_of_the_environments() -> None:
    # by default the current uv image of the python of the service, pinned by its
    # digest, mirrored into the registry of the cluster
    assert DEFAULT_BASE_IMAGE.startswith("astral-sh/uv:")
    assert "-python3.14-trixie-slim@sha256:" in DEFAULT_BASE_IMAGE
    config = ClusterConfig(registry="zot:5000")
    assert config.base_image_ref() == f"zot:5000/{DEFAULT_BASE_IMAGE}"
    # a repository of the registry, or an image with a registry host of its own
    for base, ref in (
        ("mirror/uv:1", "zot:5000/mirror/uv:1"),
        ("uv:1", "zot:5000/uv:1"),
        ("other.example:5000/uv:1", "other.example:5000/uv:1"),
        ("localhost/uv:1", "localhost/uv:1"),
    ):
        assert (
            ClusterConfig(registry="zot:5000", base_image=base).base_image_ref() == ref
        )
