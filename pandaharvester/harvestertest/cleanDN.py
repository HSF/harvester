import re
import subprocess
import sys


def clean_user_id(id):
    try:
        up = re.compile("/(DC|O|OU|C|L)=[^\/]+")
        username = up.sub("", id)
        up2 = re.compile("/CN=[0-9]+")
        username = up2.sub("", username)
        up3 = re.compile(" [0-9]+")
        username = up3.sub("", username)
        up4 = re.compile("_[0-9]+")
        username = up4.sub("", username)
        username = username.replace("/CN=proxy", "")
        username = username.replace("/CN=limited proxy", "")
        username = username.replace("limited proxy", "")
        username = re.sub("/CN=Robot:[^/]+", "", username)
        pat = re.compile(".*/CN=([^\/]+)/CN=([^\/]+)")
        mat = pat.match(username)
        if mat:
            username = mat.group(2)
        else:
            username = username.replace("/CN=", "")
        if username.lower().find("/email") > 0:
            username = username[: username.lower().find("/email")]
        pat = re.compile(".*(limited.*proxy).*")
        mat = pat.match(username)
        if mat:
            username = mat.group(1)
        username = username.replace("(", "")
        username = username.replace(")", "")
        username = username.replace("'", "")
        return username
    except BaseException:
        return id


certFile = sys.argv[1]
com = "openssl x509 -noout -subject -in"
p = subprocess.Popen(com.split() + [certFile], stdout=subprocess.PIPE)
out, err = p.communicate()

# Convert bytes object to string object if needed
try:
    out =  out.decode("utf-8")
except (UnicodeDecodeError, AttributeError):
    pass

out = re.sub("^subject=", "", out)
out = out.strip()
print(f'DN: "{out}"')
print(f'extracted: "{clean_user_id(out)}"')
