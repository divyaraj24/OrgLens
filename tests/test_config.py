from pathlib import Path
import os
from orglens.config import load_config, _interpolate

def test_load_config_with_repo_urls(tmp_path: Path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("""
repos:
  - https://github.com/django/django
  - url: https://github.com/torvalds/linux.git
  - owner: python
    repo: cpython
    git_url: https://github.com/python/cpython.git
""")
    # Change dir for loading or just pass path explicitly
    config = load_config(str(config_file))
    
    repos = config.get("repos", [])
    assert len(repos) == 3
    
    # Check the first repo (string URL)
    assert repos[0] == {"url": "https://github.com/django/django", "owner": "django", "repo": "django", "git_url": "https://github.com/django/django.git"}
    
    # Check the second repo (dict with URL only)
    assert repos[1] == {"url": "https://github.com/torvalds/linux.git", "owner": "torvalds", "repo": "linux", "git_url": "https://github.com/torvalds/linux.git"}
    
    # Check the third repo (classic config)
    assert repos[2] == {"owner": "python", "repo": "cpython", "git_url": "https://github.com/python/cpython.git"}

def test_interpolate_env_var():
    os.environ["TEST_ENV"] = "secret"
    result = _interpolate({"key": "${TEST_ENV}"})
    assert result == {"key": "secret"}
    del os.environ["TEST_ENV"]
