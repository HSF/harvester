import os
import sys
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvesterconfig import harvester_config

if not hasattr(harvester_config.watcher, "passphraseEnv"):
    print("ERROR: passphraseEnv is not defined in the watcher section of etc/panda/panda_harvester.cfg")
    sys.exit(1)

envName = harvester_config.watcher.passphraseEnv

if envName not in os.environ:
    print("ERROR: env variable {0} is undefined in etc/sysconfig/panda_harvester".format(envName))
    sys.exit(1)

key = os.environ[envName]
secret = sys.argv[1]

cipher_text = core_utils.encrypt_string(key, secret)

print("original: {0}".format(secret))
print("encrypted: {0}".format(cipher_text))

plain_text = core_utils.decrypt_string(key, cipher_text)
print("decrypted: {0}".format(plain_text))

if secret != plain_text:
    print("ERROR: the encrypted string cannot be correctly decrypted")
