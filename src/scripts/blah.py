"""Common functions for BLAH python scripts"""

import os
import subprocess

def load_env(config_dir):
    """Load blah.config into the environment"""
    load_config_path = os.path.join(config_dir, 'blah_load_config.sh')
    command = ['bash', '-c', 'source %s && env' % load_config_path]
    try:
        config_proc = subprocess.Popen(command, stdout=subprocess.PIPE)
        config_out, _ = config_proc.communicate()

        for line in config_out.splitlines():
            (key, _, val) = line.partition('=')
            os.environ[key] = val
    except IOError:
        pass
