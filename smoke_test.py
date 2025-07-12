#!/usr/bin/env python3
import subprocess, sys

def run(cmd):
    print(f'$ {" ".join(cmd)}')
    p = subprocess.run(cmd, capture_output=True, text=True)
    print(p.stdout or p.stderr)
    return p.returncode

# 1. Help tests
if run(['tng-gen-urls', '--help']) != 0: sys.exit(1)
if run(['tng-split',   '--help']) != 0: sys.exit(1)

# 2. Import test
code = "import tng_tools.fetch, tng_tools.split"
if subprocess.run(['python','-c',code]).returncode != 0:
    print('❌ Import failed'); sys.exit(1)

print('🎉 All smoke tests passed!')
