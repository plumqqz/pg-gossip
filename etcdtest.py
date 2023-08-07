import etcd3

ecl = etcd3.Etcd3Client(host='localhost', port=42379)

val = ecl.get("test")