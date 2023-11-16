import os
import sys

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils

if not hasattr(harvester_config.watcher, "passphraseEnv"):
    print("ERROR: passphraseEnv is not defined in the watcher section of etc/panda/panda_harvester.cfg")
    sys.exit(1)

envName = harvester_config.watcher.passphraseEnv

if envName not in os.environ:
    print(f"ERROR: env variable {envName} is undefined in etc/sysconfig/panda_harvester")
    sys.exit(1)

key = os.environ[envName]
secret = sys.argv[1]

cipher_text = core_utils.encrypt_string(key, secret)

print(f"original: {secret}")
print(f"encrypted: {cipher_text}")

plain_text = core_utils.decrypt_string(key, cipher_text)
print(f"decrypted: {plain_text}")

if secret != plain_text:
    print("ERROR: the encrypted string cannot be correctly decrypted")
