Create cert & key:

```
cfssl genkey -initca csr.json | cfssljson -bare ca
cfssl gencert -ca ca.pem -ca-key ca-key.pem -hostname=localhost,127.0.0.1 csr.json | cfssljson -bare server

```
