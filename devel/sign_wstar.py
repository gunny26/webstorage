#/usr/bin/python

from Crypto.PublicKey import RSA
from Crypto.Signature import PKCS1_v1_5
from Crypto.Hash import SHA256
import json

def get_signature(data, private_key_filename):
	"""
	returns string of hex signature
	data some sort of string
	private_key_filename ... path to private key
	"""
	key = open(private_key_filename, "r").read()
	rsakey = RSA.importKey(key)
	signer = PKCS1_v1_5.new(rsakey)
	digest = SHA256.new()
	digest.update(data)
	sign = signer.sign(digest)
	return sign.hex()

def signature_valid(data, signature, public_key_filename):
	"""
	verify if given signature (in hex notation) is valid

	data - some sort string data
	signature - string of hex as returned by get_signature
	public_key_filename ... path to public key in DER Format
	"""
	key = open(public_key_filename, "r").read()
	rsakey = RSA.importKey(key)
	digest = SHA256.new()
	digest.update(data)
	verifier = PKCS1_v1_5.new(rsakey)
	if verifier.verify(digest, bytes.fromhex(signature)):
		return True
	return False

if __name__ == "__main__":
	data = ["some", "list"]
	data_str = json.dumps(data).encode("utf-8")
	sig_str = get_signature(data_str, "/home/mesznera/.webstorage/private.der")
	print(signature_valid(data_str, sig_str, "/home/mesznera/.webstorage/public.der"))
